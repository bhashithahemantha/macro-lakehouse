from datetime import date, datetime

from macro_lakehouse.report import CurveSnapshot, RunReport, render_html

CURVE = CurveSnapshot(
    date(2025, 3, 6), [(3.0, "3M", 4.34), (24.0, "2Y", 3.97), (120.0, "10Y", 4.28)]
)
PASSING = [
    {
        "dataset": "treasury_yields_long",
        "name": "yield_in_range",
        "passed_records": 40,
        "failed_records": 0,
    }
]


def _report(**overrides) -> RunReport:
    base = dict(
        run_id="42",
        environment="dev",
        generated_at=datetime(2025, 3, 7, 7, 5),
        pipeline_state="COMPLETED",
        update_id="u2",
        rows_written=[{"flow": "gold_yield_curve_daily", "rows_written": 3}],
        expectations=PASSING,
        latest_curve=CURVE,
    )
    base.update(overrides)
    return RunReport(**base)


def test_healthy_report_renders_tables_and_chart():
    page = render_html(_report())
    assert "HEALTHY" in page and "yield_in_range" in page and "gold_yield_curve_daily" in page
    assert "data:image/png;base64," in page


def test_dropped_records_need_attention():
    dropped = [{**PASSING[0], "failed_records": 2}]
    page = render_html(_report(expectations=dropped))
    assert "ATTENTION NEEDED" in page and "FAIL" in page


def test_failed_pipeline_needs_attention():
    assert _report(pipeline_state="FAILED").overall_status == "ATTENTION NEEDED"


def test_report_escapes_html():
    page = render_html(
        _report(rows_written=[{"flow": "<script>x</script>", "rows_written": 1}], latest_curve=None)
    )
    assert "<script>x" not in page and "&lt;script&gt;" in page
