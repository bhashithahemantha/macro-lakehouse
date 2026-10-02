"""Download Treasury files into a landing folder (a Unity Catalog volume on Databricks)."""

import logging
from collections.abc import Iterable
from pathlib import Path

import requests

from macro_lakehouse.sources import treasury

log = logging.getLogger(__name__)


def download_years(
    target_dir: str, years: Iterable[int], session: requests.Session | None = None
) -> list[str]:
    """Download each year's CSV into `target_dir` and return the written file paths."""
    folder = Path(target_dir)
    folder.mkdir(parents=True, exist_ok=True)
    session = session or treasury.make_session()
    written = []
    for year in years:
        text = treasury.fetch_year_csv(year, session)
        path = folder / f"treasury_{year}.csv"
        path.write_text(text, encoding="utf-8")
        log.info("Wrote %s (%d bytes)", path, len(text))
        written.append(str(path))
    return written
