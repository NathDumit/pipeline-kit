import json
import os
import time

import pytest

import monitor as mon

NOW = time.time()
HOUR = 3600


def make_csv(path, rows, header="NRO_FILE,CONCAT,DT_INI", age_h=1):
    path.write_text(header + "\n" + "\n".join(f"{i},k{i},2026-01-01" for i in range(rows)) + "\n", encoding="utf-8")
    t = NOW - age_h * HOUR
    os.utime(path, (t, t))
    return path


def config(*checks, **monitor):
    return {"pipeline": {"state_dir": "state"}, "monitor": monitor, "check": list(checks)}


def run(tmp_path, cfg, **kw):
    return mon.run_checks(cfg, tmp_path, now=NOW, **kw)


def by_name(report, name):
    return next(r for r in report["results"] if r["check"] == name)


def test_healthy_file_is_ok(tmp_path):
    f = make_csv(tmp_path / "main.csv", 50)
    rep = run(tmp_path, config({"name": "main", "path": str(f), "max_age_hours": 26, "min_rows": 10,
                                "required_columns": ["NRO_FILE", "CONCAT"]}))
    assert rep["status"] == "ok"
    assert "50 rows" in by_name(rep, "main")["detail"]


def test_missing_file_fails(tmp_path):
    rep = run(tmp_path, config({"name": "x", "path": str(tmp_path / "nope.csv")}))
    assert rep["status"] == "fail" and "not found" in by_name(rep, "x")["detail"]


def test_stale_file_fails(tmp_path):
    f = make_csv(tmp_path / "old.csv", 5, age_h=60)
    rep = run(tmp_path, config({"name": "old", "path": str(f), "max_age_hours": 26}))
    assert rep["status"] == "fail" and "stale" in by_name(rep, "old")["detail"]


def test_empty_or_tiny_file_fails(tmp_path):
    f = tmp_path / "empty.csv"
    f.write_text("")
    rep = run(tmp_path, config({"name": "e", "path": str(f), "min_bytes": 10}))
    assert rep["status"] == "fail" and "too small" in by_name(rep, "e")["detail"]


def test_too_few_rows_fails(tmp_path):
    f = make_csv(tmp_path / "a.csv", 3)
    rep = run(tmp_path, config({"name": "a", "path": str(f), "min_rows": 100}))
    assert rep["status"] == "fail" and "minimum" in by_name(rep, "a")["detail"]


def test_schema_drift_is_detected(tmp_path):
    f = make_csv(tmp_path / "a.csv", 5, header="NRO_FILE,OUTRO,DT_INI")
    rep = run(tmp_path, config({"name": "a", "path": str(f), "required_columns": ["NRO_FILE", "CONCAT"]}))
    assert rep["status"] == "fail" and "CONCAT" in by_name(rep, "a")["detail"]


def test_semicolon_csv_columns_are_read(tmp_path):
    f = tmp_path / "b.csv"
    f.write_text("A;B;C\n1;2;3\n")
    assert mon.inspect_file(f) == (1, ["A", "B", "C"])


def test_silent_row_drop_is_caught_against_history(tmp_path):
    spec = {"name": "main", "path": str(tmp_path / "main.csv"), "max_row_drop_pct": 20}
    for rows in (1000, 1010, 990):                       # three good days build the baseline
        make_csv(tmp_path / "main.csv", rows)
        assert run(tmp_path, config(spec))["status"] == "ok"
    make_csv(tmp_path / "main.csv", 400)                 # half-empty extract, file is fresh and "looks" fine
    rep = run(tmp_path, config(spec))
    assert rep["status"] == "fail" and "below the recent median" in by_name(rep, "main")["detail"]


def test_bad_runs_do_not_poison_the_baseline(tmp_path):
    spec = {"name": "main", "path": str(tmp_path / "main.csv"), "max_row_drop_pct": 20}
    make_csv(tmp_path / "main.csv", 1000)
    run(tmp_path, config(spec))
    make_csv(tmp_path / "main.csv", 100)
    run(tmp_path, config(spec))                          # fails, must not be stored as normal
    hist = mon.load_history(tmp_path / "state" / "row_history.jsonl")
    assert hist["main"] == [1000]


def test_small_normal_variation_is_fine(tmp_path):
    spec = {"name": "main", "path": str(tmp_path / "main.csv"), "max_row_drop_pct": 20}
    make_csv(tmp_path / "main.csv", 1000)
    run(tmp_path, config(spec))
    make_csv(tmp_path / "main.csv", 930)
    assert run(tmp_path, config(spec))["status"] == "ok"


def test_unreadable_xlsx_is_a_failure(tmp_path):
    f = tmp_path / "broken.xlsx"
    f.write_bytes(b"this is not a zip file")
    rep = run(tmp_path, config({"name": "x", "path": str(f), "min_rows": 1}))
    assert rep["status"] == "fail" and "unreadable" in by_name(rep, "x")["detail"]


def test_parquet_row_count(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    f = tmp_path / "t.parquet"
    pd.DataFrame({"A": range(7), "B": range(7)}).to_parquet(f)
    assert mon.inspect_file(f) == (7, ["A", "B"])


# ---------------------------------------------------------------- pipeline and logs
def write_last_run(tmp_path, status="success", age_h=1, failed=()):
    d = tmp_path / "state"
    d.mkdir(exist_ok=True)
    f = d / "last_run.json"
    f.write_text(json.dumps({"run_id": "R1", "status": status, "failed_steps": list(failed)}))
    t = NOW - age_h * HOUR
    os.utime(f, (t, t))


def test_pipeline_status_checks(tmp_path):
    cfg = config(pipeline_max_age_hours=26)
    assert run(tmp_path, cfg)["status"] == "fail"                       # never ran
    write_last_run(tmp_path)
    assert run(tmp_path, cfg)["status"] == "ok"
    write_last_run(tmp_path, age_h=40)
    assert "40.0h old" in by_name(run(tmp_path, cfg), "pipeline")["detail"]
    write_last_run(tmp_path, status="failed", failed=["transform"])
    assert "transform" in by_name(run(tmp_path, cfg), "pipeline")["detail"]


def test_log_scan(tmp_path):
    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "a.log").write_text("INFO fine\nTraceback (most recent call last):\n")
    spec = {"type": "logs", "name": "logs", "glob": str(logs / "*.log"), "hours": 24, "pattern": "Traceback"}
    assert run(tmp_path, config(spec))["status"] == "fail"
    (logs / "a.log").write_text("INFO fine\n")
    assert run(tmp_path, config(spec))["status"] == "ok"
    old = NOW - 100 * HOUR
    (logs / "a.log").write_text("Traceback\n")
    os.utime(logs / "a.log", (old, old))
    assert run(tmp_path, config(spec))["status"] == "ok"                # old logs are ignored


# ---------------------------------------------------------------- alerts and outputs
def test_alert_only_when_something_is_wrong(tmp_path):
    sent = []
    good = make_csv(tmp_path / "g.csv", 5)
    run(tmp_path, config({"name": "g", "path": str(good)}), webhook="http://x", post=lambda req, timeout: sent.append(req))
    assert sent == []
    rep = run(tmp_path, config({"name": "m", "path": str(tmp_path / "missing.csv")}),
              webhook="http://x", post=lambda req, timeout: sent.append(req))
    assert len(sent) == 1 and rep["alert_sent"] is True
    assert "not found" in json.loads(sent[0].data)["text"]


def test_alert_failure_does_not_hide_the_result(tmp_path):
    def boom(req, timeout):
        raise OSError("network down")
    rep = run(tmp_path, config({"name": "m", "path": str(tmp_path / "missing.csv")}), webhook="http://x", post=boom)
    assert rep["status"] == "fail" and rep["alert_sent"] is False


def test_status_files_are_written(tmp_path):
    f = make_csv(tmp_path / "g.csv", 5)
    run(tmp_path, config({"name": "g", "path": str(f)}))
    assert json.loads((tmp_path / "state" / "status.json").read_text())["status"] == "ok"
    assert "| g | OK |" in (tmp_path / "state" / "status.md").read_text()


def test_environment_variables_in_paths(tmp_path, monkeypatch):
    make_csv(tmp_path / "e.csv", 5)
    monkeypatch.setenv("PIPELINE_OUT", str(tmp_path))
    assert run(tmp_path, config({"name": "e", "path": "${PIPELINE_OUT}/e.csv"}))["status"] == "ok"


def test_unset_environment_variable_in_a_path_is_a_named_failure_not_a_crash(tmp_path, monkeypatch):
    monkeypatch.delenv("NOT_SET_ANYWHERE", raising=False)
    cfg = {"pipeline": {}, "check": [{"name": "main", "path": "${NOT_SET_ANYWHERE}/main.parquet"}]}
    out = mon.run_checks(cfg, tmp_path)
    assert out["status"] == "fail"
    assert "NOT_SET_ANYWHERE" in out["results"][0]["detail"]
