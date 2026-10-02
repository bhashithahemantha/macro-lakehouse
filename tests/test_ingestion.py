from unittest.mock import MagicMock

from macro_lakehouse.entrypoints import parse_args
from macro_lakehouse.ingestion import download_years

CSV = 'Date,"1 Mo"\n03/06/2025,4.36\n'


def test_download_years_writes_one_file_per_year(tmp_path):
    session = MagicMock()
    session.get.return_value.text = CSV
    written = download_years(str(tmp_path / "landing"), [2024, 2025], session)
    assert [p.split("/")[-1] for p in written] == ["treasury_2024.csv", "treasury_2025.csv"]
    assert (tmp_path / "landing" / "treasury_2025.csv").read_text() == CSV
    assert session.get.call_count == 2


def test_parse_args_reads_job_parameters():
    args = parse_args(
        ["--catalog", "workspace", "--schema", "s", "--run-id", "42", "--start-year", "2024"]
    )
    assert args.catalog == "workspace"
    assert args.run_id == "42"
    assert args.start_year == 2024
    assert args.environment == "dev"


def test_parse_args_ignores_unknown_parameters():
    args = parse_args(["--catalog", "c", "--schema", "s", "--run-id", "1", "--extra", "x"])
    assert args.catalog == "c"
