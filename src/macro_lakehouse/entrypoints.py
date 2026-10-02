"""Console entry points called by the Databricks job (python_wheel_task).

Entry points only parse arguments and wire things together; the logic lives in
other modules so it can be unit tested.
"""

import argparse
import logging
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from macro_lakehouse import observability
from macro_lakehouse.config import PipelineContext
from macro_lakehouse.ingestion import download_years
from macro_lakehouse.report import CurveSnapshot, RunReport, render_html

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


def _curve_snapshot(
    spark: SparkSession, table: str, on_or_before: date | None = None
) -> CurveSnapshot | None:
    """The full curve for the latest date in `table` (optionally on or before a date)."""
    df = spark.table(table)
    if on_or_before is not None:
        df = df.where(F.col("curve_date") <= F.lit(on_or_before))
    latest = df.agg(F.max("curve_date")).first()[0]
    if latest is None:
        return None
    rows = (
        spark.table(table)
        .where(F.col("curve_date") == F.lit(latest))
        .select("maturity_months", "maturity_label", "yield_pct")
        .collect()
    )
    return CurveSnapshot(latest, [(r[0], r[1], r[2]) for r in rows])


def report() -> None:
    """Job task 3: write an HTML run report. Runs even if the pipeline failed."""
    args = parse_args()
    ctx = PipelineContext(args.catalog, args.schema)
    spark = SparkSession.builder.getOrCreate()
    run = RunReport(str(args.run_id), args.environment, datetime.now(UTC))

    if spark.catalog.tableExists(ctx.event_log_table):
        events = spark.table(ctx.event_log_table)
        update = observability.latest_update(events)
        run.update_id, run.pipeline_state = update.update_id, update.state
        if update.update_id:
            run.expectations = observability.expectation_results(events, update.update_id)
            run.rows_written = observability.rows_written(events, update.update_id)

    if spark.catalog.tableExists(ctx.silver_table):
        run.latest_curve = _curve_snapshot(spark, ctx.silver_table)
        if run.latest_curve:
            month_earlier = run.latest_curve.curve_date - timedelta(days=30)
            run.comparison_curve = _curve_snapshot(spark, ctx.silver_table, month_earlier)

    page = render_html(run)
    reports = Path(args.output_dir or ctx.reports_path)
    reports.mkdir(parents=True, exist_ok=True)
    for name in (f"run_report_{args.run_id}.html", "latest.html"):
        (reports / name).write_text(page, encoding="utf-8")
    log.info("Report written to %s (status: %s)", reports, run.overall_status)