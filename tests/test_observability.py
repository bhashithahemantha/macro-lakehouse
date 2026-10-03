import json
from datetime import datetime

import pytest

from macro_lakehouse import observability as obs

EVENT_SCHEMA = (
    "event_type string, timestamp timestamp, "
    "origin struct<update_id:string, flow_name:string>, details string"
)


def _event(event_type, ts, update_id, flow=None, details=None):
    return (event_type, ts, (update_id, flow), json.dumps(details or {}))


@pytest.fixture
def events(spark):
    exp = [
        {
            "name": "yield_in_range",
            "dataset": "treasury_yields_long",
            "passed_records": 40,
            "failed_records": 2,
        }
    ]
    rows = [
        _event(
            "update_progress",
            datetime(2025, 3, 6, 7, 0),
            "old",
            details={"update_progress": {"state": "COMPLETED"}},
        ),
        _event(
            "update_progress",
            datetime(2025, 3, 7, 7, 0),
            "u2",
            details={"update_progress": {"state": "RUNNING"}},
        ),
        _event(
            "flow_progress",
            datetime(2025, 3, 7, 7, 1),
            "u2",
            "treasury_yields_long",
            {
                "flow_progress": {
                    "metrics": {"num_output_rows": 30},
                    "data_quality": {"expectations": exp},
                }
            },
        ),
        _event(
            "flow_progress",
            datetime(2025, 3, 7, 7, 2),
            "u2",
            "treasury_yields_long",
            {"flow_progress": {"metrics": {"num_output_rows": 12}}},
        ),
        _event(
            "flow_progress",
            datetime(2025, 3, 6, 7, 1),
            "old",
            "gold_yield_curve_daily",
            {"flow_progress": {"metrics": {"num_output_rows": 999}}},
        ),
        _event(
            "update_progress",
            datetime(2025, 3, 7, 7, 5),
            "u2",
            details={"update_progress": {"state": "COMPLETED"}},
        ),
    ]
    return spark.createDataFrame(rows, EVENT_SCHEMA)


def test_latest_update_uses_final_state(events):
    assert obs.latest_update(events) == obs.UpdateSummary("u2", "COMPLETED")


def test_latest_update_handles_empty_log(spark):
    assert obs.latest_update(spark.createDataFrame([], EVENT_SCHEMA)).state == "UNKNOWN"


def test_expectation_results_for_update(events):
    assert obs.expectation_results(events, "u2") == [
        {
            "dataset": "treasury_yields_long",
            "name": "yield_in_range",
            "passed_records": 40,
            "failed_records": 2,
        }
    ]


def test_rows_written_ignores_other_updates(events):
    assert obs.rows_written(events, "u2") == [{"flow": "treasury_yields_long", "rows_written": 42}]
