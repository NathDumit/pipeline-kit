"""Small dependency-aware pipeline runner (standard library only).

Reads pipeline.toml, runs the steps in dependency order, and for every step:
  * retries with exponential backoff,
  * enforces a timeout,
  * writes a log file per step and per run,
  * skips steps whose dependencies failed,
  * can restrict steps to some weekdays (e.g. the weekly forecast on Mondays),
  * gives each step its own environment variables (paths differ per step; see pipeline.toml).
It writes state/last_run.json (read by the monitor) and appends to state/history.jsonl.
A lock file stops two runs from overlapping.

  python run_pipeline.py                      # run everything that is due today
  python run_pipeline.py --only transform   # only these steps (dependencies are assumed up to date)
  python run_pipeline.py --dry-run            # show the plan, run nothing

Exit codes: 0 = all ok, 1 = at least one step failed, 2 = bad configuration, 3 = another run is active.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import re
import subprocess
import sys
import time
import tomllib
from pathlib import Path

log = logging.getLogger("pipeline")

WEEKDAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


class ConfigError(Exception):
    pass


class LockedError(Exception):
    pass


# --------------------------------------------------------------------------- configuration
_VAR = re.compile(r"\$\{(\w+)\}|%(\w+)%")


def expand(text: str, env=None) -> str:
    """Expand ${NAME} and %NAME% from the environment. Unlike os.path.expandvars, an undefined variable is an
    error: it used to stay as the literal text "${NAME}" and the step then looked for files in a path that does not exist."""
    env = os.environ if env is None else env
    missing = []

    def repl(m):
        name = m.group(1) or m.group(2)
        if name in env:
            return env[name]
        missing.append(name)
        return m.group(0)

    out = _VAR.sub(repl, str(text))
    if missing:
        raise ConfigError("environment variable(s) not set: " + ", ".join(sorted(set(missing)))
                          + " (set them in this terminal before running)")
    return out


def load_config(path: Path) -> dict:
    with open(path, "rb") as f:
        cfg = tomllib.load(f)
    cfg.setdefault("pipeline", {})
    cfg["steps"] = cfg.pop("step", [])
    validate(cfg["steps"])
    return cfg


def validate(steps: list[dict]) -> None:
    names = [s.get("name") for s in steps]
    if not steps:
        raise ConfigError("no [[step]] defined")
    if len(set(names)) != len(names) or None in names:
        raise ConfigError("every step needs a unique name")
    for s in steps:
        if not s.get("cmd") or not isinstance(s["cmd"], list):
            raise ConfigError(f"step {s['name']}: cmd must be a non-empty list")
        for d in s.get("depends_on", []):
            if d not in names:
                raise ConfigError(f"step {s['name']}: unknown dependency {d!r}")
        for w in s.get("weekdays", []):
            if w not in WEEKDAYS:
                raise ConfigError(f"step {s['name']}: invalid weekday {w!r}")
    order_steps(steps)  # raises on cycles


def order_steps(steps: list[dict]) -> list[dict]:
    """Topological order (Kahn), keeping the file order among independent steps."""
    by_name = {s["name"]: s for s in steps}
    pending = {s["name"]: set(s.get("depends_on", [])) for s in steps}
    ordered: list[dict] = []
    while pending:
        ready = [n for n in pending if not pending[n]]
        if not ready:
            raise ConfigError("dependency cycle between steps: " + ", ".join(pending))
        n = ready[0]
        ordered.append(by_name[n])
        del pending[n]
        for deps in pending.values():
            deps.discard(n)
    return ordered


def is_due(step: dict, today: dt.date) -> bool:
    days = step.get("weekdays")
    return not days or WEEKDAYS[today.weekday()] in days


# --------------------------------------------------------------------------- lock
def acquire_lock(path: Path, max_hours: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and time.time() - path.stat().st_mtime > max_hours * 3600:
        log.warning("removing stale lock %s", path)
        path.unlink()
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        raise LockedError(f"another run is active (lock file {path})") from None
    with os.fdopen(fd, "w") as f:
        f.write(f"{os.getpid()} {dt.datetime.now().isoformat()}\n")


def release_lock(path: Path) -> None:
    path.unlink(missing_ok=True)


# --------------------------------------------------------------------------- execution
def execute(step: dict, cwd: Path, log_path: Path, sleep=time.sleep, python: str = sys.executable) -> dict:
    cmd = list(step["cmd"])
    if cmd[0] == "python":
        cmd[0] = python
    retries = int(step.get("retries", 0))
    delay = float(step.get("retry_delay_s", 30))
    timeout = step.get("timeout_s")
    env = {**os.environ, "PYTHONIOENCODING": "utf-8",
           **{k: expand(v) for k, v in step.get("env", {}).items()}}   # per-step settings
    started = time.monotonic()
    reason, rc, attempt = None, None, 0
    log_path.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(1, retries + 2):
        with open(log_path, "a", encoding="utf-8") as lf:
            lf.write(f"--- attempt {attempt} at {dt.datetime.now().isoformat(timespec='seconds')}\n")
            lf.flush()
            try:
                rc = subprocess.run(cmd, cwd=cwd, stdout=lf, stderr=subprocess.STDOUT,
                                    timeout=timeout, env=env).returncode
                reason = None if rc == 0 else f"exit code {rc}"
            except subprocess.TimeoutExpired:
                rc, reason = None, f"timeout after {timeout}s"
            except OSError as e:
                rc, reason = None, f"could not start: {e}"
        if rc == 0:
            break
        log.warning("step %s attempt %d/%d failed: %s", step["name"], attempt, retries + 1, reason)
        if attempt <= retries:
            sleep(delay * 2 ** (attempt - 1))
    return {
        "status": "success" if rc == 0 else "failed",
        "attempts": attempt,
        "returncode": rc,
        "reason": reason,
        "duration_s": round(time.monotonic() - started, 2),
        "log": str(log_path),
    }


def run_pipeline(cfg: dict, base_dir: Path, only: list[str] | None = None, dry_run: bool = False,
                 today: dt.date | None = None, sleep=time.sleep, python: str = sys.executable) -> dict:
    p = cfg["pipeline"]
    root = (base_dir / p.get("root", ".")).resolve()
    state_dir = base_dir / p.get("state_dir", "state")
    log_dir = base_dir / p.get("log_dir", "logs")
    today = today or dt.date.today()
    steps = order_steps(cfg["steps"])
    if only:
        unknown = set(only) - {s["name"] for s in steps}
        if unknown:
            raise ConfigError(f"unknown steps in --only: {sorted(unknown)}")
    for s in steps:   # fail before running anything if a step needs a variable that is not set
        if (not only or s["name"] in only) and is_due(s, today):
            for v in s.get("env", {}).values():
                expand(v)

    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S")
    report = {"run_id": run_id, "started_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
              "dry_run": dry_run, "steps": {}}
    if not dry_run:
        acquire_lock(state_dir / "pipeline.lock", float(p.get("lock_max_hours", 6)))
    try:
        for s in steps:
            name = s["name"]
            if only and name not in only:
                report["steps"][name] = {"status": "not_selected"}
                continue
            if not is_due(s, today):
                report["steps"][name] = {"status": "not_due"}
                continue
            blocked = [d for d in s.get("depends_on", [])
                       if report["steps"].get(d, {}).get("status") in ("failed", "skipped")]
            if blocked:
                report["steps"][name] = {"status": "skipped", "reason": "dependency failed: " + ", ".join(blocked)}
                log.error("step %s skipped (%s)", name, report["steps"][name]["reason"])
                continue
            if dry_run:
                report["steps"][name] = {"status": "dry_run", "cmd": s["cmd"]}
                continue
            log.info("running %s", name)
            res = execute(s, root, log_dir / run_id / f"{name}.log", sleep=sleep, python=python)
            report["steps"][name] = res
            log.info("step %s: %s in %.1fs (%d attempt(s))", name, res["status"], res["duration_s"], res["attempts"])
    finally:
        if not dry_run:
            release_lock(state_dir / "pipeline.lock")

    failed = [n for n, r in report["steps"].items() if r["status"] in ("failed", "skipped")]
    report["status"] = "failed" if failed else "success"
    report["failed_steps"] = failed
    report["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    if not dry_run:
        state_dir.mkdir(parents=True, exist_ok=True)
        (state_dir / "last_run.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        with open(state_dir / "history.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps(report, ensure_ascii=False) + "\n")
    return report


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=str(Path(__file__).resolve().parents[1] / "pipeline.toml"))
    ap.add_argument("--only", help="comma-separated step names")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg_path = Path(args.config).resolve()
    try:
        cfg = load_config(cfg_path)
        report = run_pipeline(cfg, cfg_path.parent, only=args.only.split(",") if args.only else None,
                              dry_run=args.dry_run)
    except ConfigError as e:
        log.error("configuration error: %s", e)
        return 2
    except LockedError as e:
        log.error("%s", e)
        return 3
    for name, r in report["steps"].items():
        log.info("%-24s %s", name, r["status"])
    return 0 if report["status"] == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
