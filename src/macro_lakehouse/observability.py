"""Summarise a declarative pipeline's event log for the run report.

The pipeline publishes its event log to a Unity Catalog table (configured in the bundle).
Expectation results and rows written per flow are recorded there as JSON in `details`.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

EXPECTATIONS_SCHEMA = (
    "array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>"
)


@dataclass(frozen=True)
class UpdateSummary:
    update_id: str | None
    state: str


def latest_update(events: DataFrame) -> UpdateSummary:
    """The most recent pipeline update and its final state (COMPLETED, FAILED, ...)."""
    progress = events.where(F.col("event_type") == "update_progress").select(
        F.col("origin.update_id").alias("update_id"),
        F.get_json_object("details", "$.update_progress.state").alias("state"),
        "timestamp",
    )
    row = progress.orderBy(F.col("timestamp").desc()).first()
    return UpdateSummary(None, "UNKNOWN") if row is None else UpdateSummary(row[0], row[1])


def _flow_progress(events: DataFrame, update_id: str) -> DataFrame:
    return events.where(
        (F.col("event_type") == "flow_progress") & (F.col("origin.update_id") == update_id)
    )


def expectation_results(events: DataFrame, update_id: str) -> list[dict]:
    """Passed / failed record counts per expectation for one update."""
    parsed = _flow_progress(events, update_id).select(
        F.explode(
            F.from_json(
                F.get_json_object("details", "$.flow_progress.data_quality.expectations"),
                EXPECTATIONS_SCHEMA,
            )
        ).alias("e")
    )
    summary = (
        parsed.groupBy(F.col("e.dataset").alias("dataset"), F.col("e.name").alias("name"))
        .agg(
            F.sum("e.passed_records").alias("passed_records"),
            F.sum("e.failed_records").alias("failed_records"),
        )
        .orderBy("dataset", "name")
    )
    return [r.asDict() for r in summary.collect()]


def rows_written(events: DataFrame, update_id: str) -> list[dict]:
    """Output rows per flow (dataset) for one update."""
    summary = (
        _flow_progress(events, update_id)
        .select(
            F.col("origin.flow_name").alias("flow"),
            F.get_json_object("details", "$.flow_progress.metrics.num_output_rows")
            .cast("long")
            .alias("rows"),
        )
        .where(F.col("rows").isNotNull())
        .groupBy("flow")
        .agg(F.sum("rows").alias("rows_written"))
        .orderBy("flow")
    )
    return [r.asDict() for r in summary.collect()]
