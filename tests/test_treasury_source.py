from unittest.mock import MagicMock

import pytest
import requests

from macro_lakehouse.sources import treasury

@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Date", "date"),
        ("1 Mo", "t_1_mo"),
        ("1.5 Month", "t_1_5_month"),
        ("10 Yr", "t_10_yr"),
        ("\ufeffDate", "date"),  # byte-order mark some files start with
    ],
)
def test_sanitize_column(raw, expected):
    assert treasury.sanitize_column(raw) == expected


@pytest.mark.parametrize(
    ("column", "months"),
    [
        ("t_1_mo", 1.0),
        ("t_1_5_month", 1.5),
        ("t_2_yr", 24.0),
        ("t_30_yr", 360.0),
        ("date", None),          # not a tenor
        ("_source_file", None),  # not a tenor
    ],
)
def test_tenor_months(column, months):
    assert treasury.tenor_months(column) == months


@pytest.mark.parametrize(("months", "label"), [(3.0, "3M"), (1.5, "1.5M"), (24.0, "2Y")])
def test_tenor_label(months, label):
    assert treasury.tenor_label(months) == label

def test_build_url_contains_year():
    url = treasury.build_url(2025)
    assert "/2025/all" in url
    assert "field_tdr_date_value=2025" in url


def test_validate_csv_accepts_treasury_header():
    treasury.validate_csv('Date,"1 Mo","3 Mo"\n03/06/2025,4.36,4.34\n')


def test_validate_csv_rejects_html_error_page():
    with pytest.raises(ValueError, match="Unexpected Treasury response"):
        treasury.validate_csv("<html><body>Service unavailable</body></html>")


def test_validate_csv_rejects_empty_response():
    with pytest.raises(ValueError):
        treasury.validate_csv("")


def test_session_retries_transient_errors_only():
    retry = treasury.make_session().get_adapter("https://").max_retries
    assert retry.total == 3
    assert 503 in retry.status_forcelist
    assert 404 not in retry.status_forcelist


def _fake_session(text: str) -> MagicMock:
    session = MagicMock()
    session.get.return_value.text = text
    return session


def test_fetch_year_csv_returns_validated_text():
    session = _fake_session('Date,"1 Mo"\n03/06/2025,4.36\n')
    text = treasury.fetch_year_csv(2025, session)
    assert text.startswith("Date")
    session.get.assert_called_once_with(treasury.build_url(2025), timeout=30)
    session.get.return_value.raise_for_status.assert_called_once()


def test_fetch_year_csv_raises_on_http_error():
    session = _fake_session("")
    session.get.return_value.raise_for_status.side_effect = requests.HTTPError("503")
    with pytest.raises(requests.HTTPError):
        treasury.fetch_year_csv(2025, session)