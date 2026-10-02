"""Client and parsing helpers for the US Treasury daily par yield curve CSV feed.

Two responsibilities live here:
1. Downloading one year of data reliably (retries, timeouts, response validation).
2. Turning Treasury's messy column headers into clean names, maturities and labels.
"""

import logging
import re

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

# ---------------------------------------------------------------- download

BASE_URL = (
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/"
    "daily-treasury-rates.csv/{year}/all"
)
QUERY = "type=daily_treasury_yield_curve&field_tdr_date_value={year}&page&_format=csv"


def build_url(year: int) -> str:
    """URL of the CSV for one calendar year.

    >>> build_url(2025).endswith("field_tdr_date_value=2025&page&_format=csv")
    True
    """
    return f"{BASE_URL.format(year=year)}?{QUERY.format(year=year)}"


def make_session(retries: int = 3, backoff: float = 1.0) -> requests.Session:
    """HTTP session that retries transient failures with exponential backoff.

    Retries on 429 (rate limited) and 5xx (server errors), waiting roughly
    1s, 2s, 4s between attempts. Client errors such as 404 are not retried,
    because trying again would not help.
    """
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = "macro-lakehouse/0.1 (portfolio project)"
    return session


def validate_csv(text: str) -> None:
    """Fail fast if the response is not the CSV we expect.

    Servers sometimes return an HTML error page with status 200. Writing that
    into the lake would silently corrupt bronze, so we check the header first.
    """
    first_line = text.lstrip("\ufeff").splitlines()[0] if text.strip() else ""
    if not first_line.startswith("Date"):
        raise ValueError(f"Unexpected Treasury response, header was: {first_line[:80]!r}")


def fetch_year_csv(year: int, session: requests.Session | None = None, timeout: int = 30) -> str:
    """Download and validate one year of daily par yield curve rates as CSV text."""
    session = session or make_session()
    url = build_url(year)
    log.info("Fetching %s", url)
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    text = response.text
    validate_csv(text)
    return text


# ---------------------------------------------------------------- header parsing

_TENOR_PATTERN = re.compile(r"^t_(\d+(?:_\d+)?)_(mo|month|months|yr|year|years)$")


def sanitize_column(name: str) -> str:
    """Turn a raw header into a Delta-safe column name.

    >>> sanitize_column("1 Mo")
    't_1_mo'
    >>> sanitize_column("1.5 Month")
    't_1_5_month'
    >>> sanitize_column("Date")
    'date'
    """
    cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip().lstrip("\ufeff")).strip("_").lower()
    return f"t_{cleaned}" if cleaned[:1].isdigit() else cleaned


def tenor_months(column: str) -> float | None:
    """Maturity in months for a sanitized tenor column, or None if it is not a tenor.

    >>> tenor_months("t_1_5_month")
    1.5
    >>> tenor_months("t_10_yr")
    120.0
    >>> tenor_months("date") is None
    True
    """
    match = _TENOR_PATTERN.match(column)
    if not match:
        return None
    value = float(match.group(1).replace("_", "."))
    unit = match.group(2)
    return value * 12 if unit.startswith(("yr", "year")) else value


def tenor_label(months: float) -> str:
    """Human-friendly maturity label.

    >>> tenor_label(3.0)
    '3M'
    >>> tenor_label(1.5)
    '1.5M'
    >>> tenor_label(24.0)
    '2Y'
    """
    if months >= 12 and months % 12 == 0:
        return f"{int(months // 12)}Y"
    return f"{months:g}M"