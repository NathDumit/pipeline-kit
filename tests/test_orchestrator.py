"""The runner is tested with tiny throw-away scripts, so no real data or network is involved."""
import datetime as dt
import json
import sys
import textwrap

import pytest

import run_pipeline as rp

PY = "python"


def write(path, code):
    path.write_text(textwrap.dedent(code), encoding="utf-8")
    return path.name


def cfg(steps, **pipeline):
    return {"pipeline": {"root": ".", "lock_max_hours": 1, **pipeline}, "steps": steps}


def run(tmp_path, steps, **kw):
    return rp.run_pipeline(cfg(steps), tmp_path, sleep=lambda s: None, **kw)


# ---------------------------------------------------------------- configuration
def test_order_respects_dependencies():
    steps = [{"name": "c", "cmd": ["x"], "depends_on": ["b"]},
             {"name": "a", "cmd": ["x"]},
             {"name": "b", "cmd": ["x"], "depends_on": ["a"]}]
    assert [s["name"] for s in rp.order_steps(steps)] == ["a", "b", "c"]


def test_cycle_is_rejected():
    steps = [{"name": "a", "cmd": ["x"], "depends_on": ["b"]}, {"name": "b", "cmd": ["x"], "depends_on": ["a"]}]
    with pytest.raises(rp.ConfigError, match="cycle"):
        rp.validate(steps)


@pytest.mark.parametrize("steps, message", [
    ([], "no \\[\\[step\\]\\]"),
    ([{"name": "a", "cmd": ["x"]}, {"name": "a", "cmd": ["x"]}], "unique"),
    ([{"name": "a", "cmd": ["x"], "depends_on": ["zzz"]}], "unknown dependency"),
    ([{"name": "a", "cmd": []}], "cmd"),
    ([{"name": "a", "cmd": ["x"], "weekdays": ["funday"]}], "weekday"),
])
def test_invalid_configuration(steps, message):
    with pytest.raises(rp.ConfigError, match=message):
        rp.validate(steps)


def test_example_configuration_in_the_repo_is_valid():
    path = rp.Path(__file__).resolve().parents[1] / "pipeline.toml"
    assert len(rp.load_config(path)["steps"]) >= 3


# ---------------------------------------------------------------- execution
def test_success_and_state_files(tmp_path):
    ok = write(tmp_path / "ok.py", "print('hello')")
    rep = run(tmp_path, [{"name": "ok", "cmd": [PY, ok]}])
    assert rep["status"] == "success" and rep["steps"]["ok"]["attempts"] == 1
    assert "hello" in (tmp_path / "logs" / rep["run_id"] / "ok.log").read_text()
    assert json.loads((tmp_path / "state" / "last_run.json").read_text())["status"] == "success"
    assert (tmp_path / "state" / "history.jsonl").read_text().count("\n") == 1
    assert not (tmp_path / "state" / "pipeline.lock").exists()


def test_flaky_step_succeeds_on_retry(tmp_path):
    flaky = write(tmp_path / "flaky.py", """
        import pathlib, sys
        marker = pathlib.Path("attempts.txt")
        n = int(marker.read_text()) if marker.exists() else 0
        marker.write_text(str(n + 1))
        sys.exit(1 if n == 0 else 0)
    """)
    rep = run(tmp_path, [{"name": "flaky", "cmd": [PY, flaky], "retries": 2}])
    assert rep["steps"]["flaky"]["status"] == "success"
    assert rep["steps"]["flaky"]["attempts"] == 2


def test_backoff_doubles_the_delay(tmp_path):
    bad = write(tmp_path / "bad.py", "import sys; sys.exit(3)")
    waits = []
    rp.run_pipeline(cfg([{"name": "bad", "cmd": [PY, bad], "retries": 3, "retry_delay_s": 5}]), tmp_path,
                    sleep=waits.append)
    assert waits == [5, 10, 20]


def test_failure_skips_dependents_but_not_independent_steps(tmp_path):
    bad = write(tmp_path / "bad.py", "import sys; sys.exit(2)")
    ok = write(tmp_path / "ok.py", "pass")
    rep = run(tmp_path, [
        {"name": "base", "cmd": [PY, bad]},
        {"name": "child", "cmd": [PY, ok], "depends_on": ["base"]},
        {"name": "grandchild", "cmd": [PY, ok], "depends_on": ["child"]},
        {"name": "independent", "cmd": [PY, ok]},
    ])
    s = rep["steps"]
    assert s["base"]["status"] == "failed" and s["base"]["returncode"] == 2
    assert s["child"]["status"] == "skipped" and "base" in s["child"]["reason"]
    assert s["grandchild"]["status"] == "skipped"
    assert s["independent"]["status"] == "success"
    assert rep["status"] == "failed" and set(rep["failed_steps"]) == {"base", "child", "grandchild"}


def test_timeout_is_enforced(tmp_path):
    slow = write(tmp_path / "slow.py", "import time; time.sleep(30)")
    rep = run(tmp_path, [{"name": "slow", "cmd": [PY, slow], "timeout_s": 1}])
    assert rep["steps"]["slow"]["status"] == "failed"
    assert "timeout" in rep["steps"]["slow"]["reason"]


def test_missing_executable_is_a_failure_not_a_crash(tmp_path):
    rep = run(tmp_path, [{"name": "ghost", "cmd": ["definitely-not-a-program-xyz"]}])
    assert rep["steps"]["ghost"]["status"] == "failed"
    assert "could not start" in rep["steps"]["ghost"]["reason"]


def test_weekday_filter(tmp_path):
    ok = write(tmp_path / "ok.py", "pass")
    steps = [{"name": "weekly", "cmd": [PY, ok], "weekdays": ["mon"]}]
    monday, tuesday = dt.date(2026, 11, 2), dt.date(2026, 11, 3)
    assert run(tmp_path, steps, today=monday)["steps"]["weekly"]["status"] == "success"
    assert run(tmp_path, steps, today=tuesday)["steps"]["weekly"]["status"] == "not_due"


def test_step_not_due_does_not_block_its_dependents(tmp_path):
    ok = write(tmp_path / "ok.py", "pass")
    rep = run(tmp_path, [
        {"name": "weekly", "cmd": [PY, ok], "weekdays": ["mon"]},
        {"name": "daily", "cmd": [PY, ok], "depends_on": ["weekly"]},
    ], today=dt.date(2026, 11, 3))
    assert rep["steps"]["daily"]["status"] == "success"


def test_only_runs_the_selected_steps(tmp_path):
    ok = write(tmp_path / "ok.py", "pass")
    rep = run(tmp_path, [{"name": "a", "cmd": [PY, ok]}, {"name": "b", "cmd": [PY, ok]}], only=["b"])
    assert rep["steps"]["a"]["status"] == "not_selected" and rep["steps"]["b"]["status"] == "success"
    with pytest.raises(rp.ConfigError):
        run(tmp_path, [{"name": "a", "cmd": [PY, ok]}], only=["nope"])


def test_dry_run_changes_nothing(tmp_path):
    ok = write(tmp_path / "ok.py", "open('side_effect.txt', 'w').write('x')")
    rep = run(tmp_path, [{"name": "a", "cmd": [PY, ok]}], dry_run=True)
    assert rep["steps"]["a"]["status"] == "dry_run"
    assert not (tmp_path / "side_effect.txt").exists() and not (tmp_path / "state").exists()


# ---------------------------------------------------------------- lock
def test_second_run_is_refused_while_the_first_is_active(tmp_path):
    rp.acquire_lock(tmp_path / "state" / "pipeline.lock", 1)
    ok = write(tmp_path / "ok.py", "pass")
    with pytest.raises(rp.LockedError):
        run(tmp_path, [{"name": "a", "cmd": [PY, ok]}])
    assert (tmp_path / "state" / "pipeline.lock").exists()          # the active run's lock is untouched


def test_stale_lock_is_replaced(tmp_path):
    lock = tmp_path / "state" / "pipeline.lock"
    rp.acquire_lock(lock, 1)
    old = lock.stat().st_mtime - 3 * 3600
    import os
    os.utime(lock, (old, old))
    ok = write(tmp_path / "ok.py", "pass")
    assert run(tmp_path, [{"name": "a", "cmd": [PY, ok]}])["status"] == "success"


def test_lock_is_released_even_if_the_runner_raises(tmp_path):
    ok = write(tmp_path / "ok.py", "pass")
    with pytest.raises(rp.ConfigError):
        run(tmp_path, [{"name": "a", "cmd": [PY, ok]}], only=["unknown"])
    assert not (tmp_path / "state" / "pipeline.lock").exists()


def test_main_exit_codes(tmp_path):
    ok = write(tmp_path / "ok.py", "pass")
    bad = write(tmp_path / "bad.py", "import sys; sys.exit(1)")
    good_cfg = tmp_path / "good.toml"
    good_cfg.write_text(f'[pipeline]\n[[step]]\nname="a"\ncmd=["python","{ok}"]\n')
    bad_cfg = tmp_path / "bad.toml"
    bad_cfg.write_text(f'[pipeline]\n[[step]]\nname="a"\ncmd=["python","{bad}"]\n')
    broken = tmp_path / "broken.toml"
    broken.write_text("[pipeline]\n")
    assert rp.main(["--config", str(good_cfg)]) == 0
    assert rp.main(["--config", str(bad_cfg)]) == 1
    assert rp.main(["--config", str(broken)]) == 2


# ---------------------------------------------------------------- environment variables in step settings
def test_expand_replaces_both_styles_and_rejects_undefined_variables():
    env = {"OUT": "C:\\x"}
    assert rp.expand("${OUT}/a", env) == "C:\\x/a"
    assert rp.expand("%OUT%\\a", env) == "C:\\x\\a"
    with pytest.raises(rp.ConfigError, match="MISSING"):
        rp.expand("${MISSING}/a", env)


def test_undefined_variable_stops_the_run_before_any_step_and_leaves_no_lock(tmp_path, monkeypatch):
    monkeypatch.delenv("NOT_SET_ANYWHERE", raising=False)
    marker = tmp_path / "ran.txt"
    code = write(tmp_path / "s.py", f"open(r'{marker}', 'w').write('x')")
    steps = [{"name": "s", "cmd": [PY, code], "env": {"X": "${NOT_SET_ANYWHERE}"}}]
    with pytest.raises(rp.ConfigError, match="NOT_SET_ANYWHERE"):
        run(tmp_path, steps)
    assert not marker.exists() and not (tmp_path / "state" / "pipeline.lock").exists()


def test_defined_variable_reaches_the_step(tmp_path, monkeypatch):
    monkeypatch.setenv("MY_OUT", "hello-path")
    code = write(tmp_path / "s.py", "import os; print('GOT=' + os.environ['X'])")
    rep = run(tmp_path, [{"name": "s", "cmd": [PY, code], "env": {"X": "${MY_OUT}"}}])
    assert rep["status"] == "success"
    assert "GOT=hello-path" in (tmp_path / "logs" / rep["run_id"] / "s.log").read_text()
