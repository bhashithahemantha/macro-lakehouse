"""Daily pipeline run report: a self-contained HTML page for the data team.

Rendering is kept free of Spark so it can be unit tested with plain Python data.
"""

from __future__ import annotations

import base64
import html
import io
from dataclasses import dataclass, field
from datetime import date, datetime

import matplotlib

matplotlib.use("Agg")  # headless rendering on Databricks and in CI
import matplotlib.pyplot as plt  # noqa: E402


@dataclass
class CurveSnapshot:
    curve_date: date
    points: list[tuple[float, str, float]]  # (maturity_months, label, yield_pct)


@dataclass
class RunReport:
    run_id: str
    environment: str
    generated_at: datetime
    pipeline_state: str = "UNKNOWN"
    update_id: str | None = None
    rows_written: list[dict] = field(default_factory=list)
    expectations: list[dict] = field(default_factory=list)
    latest_curve: CurveSnapshot | None = None
    comparison_curve: CurveSnapshot | None = None

    @property
    def records_dropped_or_flagged(self) -> int:
        return sum(int(e.get("failed_records") or 0) for e in self.expectations)

    @property
    def overall_status(self) -> str:
        ok = self.pipeline_state == "COMPLETED" and self.records_dropped_or_flagged == 0
        return "HEALTHY" if ok else "ATTENTION NEEDED"


def render_curve_chart(latest: CurveSnapshot, comparison: CurveSnapshot | None) -> str:
    """Return the yield curve chart as a base64 PNG for embedding in HTML."""
    fig, ax = plt.subplots(figsize=(8, 3.6), dpi=110)
    for snap, style in ((latest, "-o"), (comparison, "--o")):
        if snap is None:
            continue
        pts = sorted(snap.points)
        ax.plot(
            [p[0] for p in pts],
            [p[2] for p in pts],
            style,
            label=str(snap.curve_date),
            markersize=4,
            linewidth=1.8,
        )
    pts = sorted(latest.points)
    ax.set_xscale("log")
    ax.set_xticks([p[0] for p in pts])
    ax.set_xticklabels([p[1] for p in pts], fontsize=8)
    ax.set_ylabel("Yield (%)")
    ax.set_title("US Treasury par yield curve")
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png")
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def _badge(ok: bool) -> str:
    return '<span class="ok">PASS</span>' if ok else '<span class="bad">FAIL</span>'


def _fmt(value) -> str:
    return "" if value is None else html.escape(str(value))


def render_html(report: RunReport) -> str:
    flow_rows = "".join(
        f"<tr><td>{_fmt(r['flow'])}</td><td>{_fmt(r['rows_written'])}</td></tr>"
        for r in report.rows_written
    )
    dq_rows = "".join(
        f"<tr><td>{_fmt(e['dataset'])}</td><td>{_fmt(e['name'])}</td>"
        f"<td>{_badge(not e['failed_records'])}</td><td>{_fmt(e['passed_records'])}</td>"
        f"<td>{_fmt(e['failed_records'])}</td></tr>"
        for e in report.expectations
    )
    chart = ""
    if report.latest_curve:
        png = render_curve_chart(report.latest_curve, report.comparison_curve)
        chart = f'<h2>Yield curve</h2><img alt="yield curve" src="data:image/png;base64,{png}"/>'

    status_class = "ok" if report.overall_status == "HEALTHY" else "bad"
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>macro-lakehouse run {html.escape(report.run_id)}</title>
<style>
 body {{ font-family: -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; color: #1f2933; }}
 table {{ border-collapse: collapse; width: 100%; margin-bottom: 1.5rem; font-size: 14px; }}
 th, td {{ border-bottom: 1px solid #e4e7eb; padding: 6px 10px; text-align: left; }}
 th {{ background: #f5f7fa; }}
 .ok {{ color: #0b7a3e; font-weight: 600; }} .bad {{ color: #c0392b; font-weight: 600; }}
 .meta {{ color: #616e7c; }} img {{ max-width: 100%; }}
</style></head><body>
<h1>Pipeline run report <span class="{status_class}">{report.overall_status}</span></h1>
<p class="meta">Job run {html.escape(report.run_id)} &middot; environment
 {html.escape(report.environment)} &middot; pipeline update {_fmt(report.update_id)}
 ({_fmt(report.pipeline_state)}) &middot; generated {report.generated_at:%Y-%m-%d %H:%M} UTC</p>
<h2>Rows written by flow</h2>
<table><tr><th>Flow</th><th>Rows written</th></tr>{flow_rows}</table>
<h2>Data quality expectations</h2>
<table><tr><th>Dataset</th><th>Expectation</th><th>Result</th><th>Passed</th><th>Failed</th></tr>
{dq_rows}</table>
{chart}
</body></html>"""