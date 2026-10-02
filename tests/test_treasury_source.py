import pytest

from macro_lakehouse.sources import treasury


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Date", "date"),
        ("1 Mo", "t_1_mo"),
        ("1.5 Month", "t_1_5_month"),
        ("10 Yr", "t_10_yr"),
        ("\ufeffDate", "date"),  # byte-order mark some files start with
    ],
)
def test_sanitize_column(raw, expected):
    assert treasury.sanitize_column(raw) == expected


@pytest.mark.parametrize(
    ("column", "months"),
    [
        ("t_1_mo", 1.0),
        ("t_1_5_month", 1.5),
        ("t_2_yr", 24.0),
        ("t_30_yr", 360.0),
        ("date", None),          # not a tenor
        ("_source_file", None),  # not a tenor
    ],
)
def test_tenor_months(column, months):
    assert treasury.tenor_months(column) == months


@pytest.mark.parametrize(("months", "label"), [(3.0, "3M"), (1.5, "1.5M"), (24.0, "2Y")])
def test_tenor_label(months, label):
    assert treasury.tenor_label(months) == label