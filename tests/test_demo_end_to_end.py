"""End to end on the bundled demo: orchestrator runs the steps, monitor verifies the outputs."""
import tomllib
from pathlib import Path

import monitor as mon
import run_pipeline as rp

ROOT = Path(__file__).resolve().parents[1]


def load(tmp_path, monkeypatch):
    monkeypatch.setenv("PIPELINE_OUT", str(tmp_path / "out"))
    cfg = tomllib.loads((ROOT / "pipeline.toml").read_text(encoding="utf-8"))
    cfg["pipeline"]["root"] = str(ROOT)
    cfg["pipeline"]["state_dir"] = str(tmp_path / "state")
    cfg["pipeline"]["log_dir"] = str(tmp_path / "logs")
    for c in cfg["check"]:
        if c.get("type") == "logs":
            c["glob"] = str(tmp_path / "logs" / "**" / "*.log")
    cfg["steps"] = cfg.pop("step")
    cfg["checks"] = cfg["check"]
    return cfg


def test_demo_pipeline_runs_and_monitor_trusts_it(tmp_path, monkeypatch):
    cfg = load(tmp_path, monkeypatch)
    report = rp.run_pipeline(cfg, tmp_path, sleep=lambda s: None)
    assert report["steps"]["extract"]["status"] == "success"
    assert report["steps"]["transform"]["status"] == "success"
    assert (tmp_path / "out" / "summary.csv").exists()
    cfg["pipeline"]["state_dir"] = "state"
    assert mon.run_checks(cfg, tmp_path)["status"] in ("ok", "warn")
