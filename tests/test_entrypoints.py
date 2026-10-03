from importlib import metadata


def test_console_scripts_resolve():
    scripts = [
        ep
        for ep in metadata.distribution("macro-lakehouse").entry_points
        if ep.group == "console_scripts"
    ]
    assert {"ingest", "report"} <= {ep.name for ep in scripts}
    for ep in scripts:
        assert callable(ep.load()), f"entry point {ep.name} does not resolve"
