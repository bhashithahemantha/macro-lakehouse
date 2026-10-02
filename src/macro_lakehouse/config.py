"""Per-environment names, derived from the catalog and schema set by the bundle target."""

from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineContext:
    """Where the pipeline reads and writes in Unity Catalog.

    The same code runs in dev and prod; only catalog/schema differ, and those come
    from the Asset Bundle target.
    """

    catalog: str
    schema: str

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"

    @property
    def landing_root(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/landing/treasury"

    def landing_path(self, run_id: str) -> str:
        return f"{self.landing_root}/run_{run_id}"

    @property
    def reports_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/reports"

    @property
    def silver_table(self) -> str:
        return self.table("silver_treasury_yields")

    @property
    def gold_table(self) -> str:
        return self.table("gold_yield_curve_daily")

    @property
    def event_log_table(self) -> str:
        return self.table("ops_pipeline_event_log")
