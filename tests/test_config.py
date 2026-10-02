from macro_lakehouse.config import PipelineContext

CTX = PipelineContext(catalog="workspace", schema="dev_bhashi_macro_lakehouse")


def test_table_names_use_catalog_and_schema():
    assert CTX.silver_table == "workspace.dev_bhashi_macro_lakehouse.silver_treasury_yields"
    assert CTX.event_log_table.endswith(".ops_pipeline_event_log")


def test_landing_path_is_unique_per_run():
    assert CTX.landing_path("99") == (
        "/Volumes/workspace/dev_bhashi_macro_lakehouse/landing/treasury/run_99"
    )
    assert CTX.landing_path("99") != CTX.landing_path("100")


def test_reports_path():
    assert CTX.reports_path == "/Volumes/workspace/dev_bhashi_macro_lakehouse/reports"
