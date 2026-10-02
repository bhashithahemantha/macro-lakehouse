"""Console entry points called by the Databricks job (python_wheel_task).

Entry points only parse arguments and wire things together; the logic lives in
other modules so it can be unit tested.
"""

import argparse
import logging
from datetime import date

from macro_lakehouse.config import PipelineContext
from macro_lakehouse.ingestion import download_years

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("macro_lakehouse")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="macro-lakehouse job task")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--environment", default="dev")
    parser.add_argument("--start-year", type=int, default=date.today().year)
    parser.add_argument("--output-dir", help="Override the landing folder (local testing)")
    args, _unknown = parser.parse_known_args(argv)
    return args


def ingest() -> None:
    """Job task 1: download every year from --start-year to this year into the landing volume."""
    args = parse_args()
    ctx = PipelineContext(args.catalog, args.schema)
    target = args.output_dir or ctx.landing_path(args.run_id)
    years = range(args.start_year, date.today().year + 1)
    written = download_years(target, years)
    log.info("Ingest complete: %d file(s) in %s", len(written), target)
