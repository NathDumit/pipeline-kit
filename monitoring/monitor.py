"""Data-quality and freshness monitor (standard library; pandas/pyarrow/openpyxl only to count rows).

Runs after the pipeline (or on its own schedule) and answers one question: "can I trust today's outputs?"
Checks, all driven by pipeline.toml:
  * the last pipeline run succeeded and is recent,
  * each output file exists, is fresh (max_age_hours), is not tiny (min_bytes),
  * row count is above min_rows and has not dropped more than max_row_drop_pct against the median of
    the last runs (a silent half-empty extract is the failure that hurts most),
  * required columns are still there (schema drift),
  * log files of the last hours contain no ERROR/Traceback lines.
It writes state/status.json and state/status.md, and can post an alert to a webhook
(env PIPELINE_ALERT_WEBHOOK) when something is not ok. Nothing is sent when everything is fine.

Exit codes: 0 = ok, 1 = warnings, 2 = failures.
"""
from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import os
import re
import statistics
import sys
import time
import tomllib
import urllib.request
from pathlib import Path

OK, WARN, FAIL = "ok", "warn", "fail"
RANK = {OK: 0, WARN: 1, FAIL: 2}


def _result(name: str, status: str, detail: str) -> dict:
    return {"check": name, "status": status, "detail": detail}


# --------------------------------------------------------------------------- reading files
def inspect_file(path: Path) -> tuple[int | None, list[str] | None]:
    """Return (row_count, column_names); None where it cannot be determined."""
    suffix = path.suffix.lower()
    try:
        if suffix in (".csv", ".txt"):
            with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
                header = f.readline().rstrip("\r\n")
                rows = sum(1 for _ in f)
            sep = ";" if header.count(";") > header.count(",") else ","
            return rows, [c.strip().strip('"') for c in header.split(sep)]
        if suffix == ".parquet":
            import pyarrow.parquet as pq
            pf = pq.ParquetFile(path)
            return pf.metadata.num_rows, list(pf.schema_arrow.names)
        if suffix in (".xlsx", ".xlsm"):
            import openpyxl
            wb = openpyxl.load_workbook(path, read_only=True)
            ws = wb.worksheets[0]
            header = [c for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True), [])]
            rows = max((ws.max_row or 1) - 1, 0)
            wb.close()
            return rows, [str(c) for c in header if c is not None]
    except ImportError:
        return None, None
    except Exception:  # unreadable/corrupt file is itself a finding, reported by the caller
        raise
    return None, None


# --------------------------------------------------------------------------- history of row counts
def load_history(path: Path) -> dict[str, list[int]]:
    hist: dict[str, list[int]] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                rec = json.loads(line)
                hist.setdefault(rec["check"], []).append(int(rec["rows"]))
            except (ValueError, KeyError):
                continue
    return hist


def append_history(path: Path, check: str, rows: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps({"check": check, "rows": rows,
                            "at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")}) + "\n")


# --------------------------------------------------------------------------- checks
def check_pipeline(state_dir: Path, max_age_hours: float, now: float) -> dict:
    f = state_dir / "last_run.json"
    if not f.exists():
        return _result("pipeline", FAIL, "no last_run.json: the pipeline has never run here")
    rep = json.loads(f.read_text(encoding="utf-8"))
    age_h = (now - f.stat().st_mtime) / 3600
    if rep.get("status") != "success":
        return _result("pipeline", FAIL, f"last run {rep.get('run_id')} failed: {', '.join(rep.get('failed_steps', []))}")
    if age_h > max_age_hours:
        return _result("pipeline", FAIL, f"last successful run is {age_h:.1f}h old (limit {max_age_hours}h)")
    return _result("pipeline", OK, f"run {rep.get('run_id')} succeeded {age_h:.1f}h ago")


_VAR = re.compile(r"\$\{(\w+)\}|%(\w+)%")


def _expand(text: str) -> str:
    """${NAME} / %NAME% from the environment; an undefined variable is reported by name, not left as literal text."""
    missing = []

    def repl(m):
        name = m.group(1) or m.group(2)
        if name in os.environ:
            return os.environ[name]
        missing.append(name)
        return m.group(0)

    out = _VAR.sub(repl, str(text))
    if missing:
        raise ValueError("environment variable(s) not set: " + ", ".join(sorted(set(missing))))
    return out


def check_file(spec: dict, history: dict[str, list[int]], now: float) -> tuple[dict, int | None]:
    name = spec.get("name") or spec["path"]
    path = Path(_expand(spec["path"]))
    if not path.exists():
        return _result(name, FAIL, f"file not found: {path}"), None
    st = path.stat()
    problems, warnings = [], []
    age_h = (now - st.st_mtime) / 3600
    if "max_age_hours" in spec and age_h > spec["max_age_hours"]:
        problems.append(f"stale: {age_h:.1f}h old (limit {spec['max_age_hours']}h)")
    if st.st_size < spec.get("min_bytes", 1):
        problems.append(f"too small: {st.st_size} bytes")

    rows = None
    wants_content = any(k in spec for k in ("min_rows", "max_row_drop_pct", "required_columns"))
    if wants_content and st.st_size > 0:
        try:
            rows, cols = inspect_file(path)
        except Exception as e:
            return _result(name, FAIL, f"unreadable ({type(e).__name__}: {e})"), None
        if rows is None:
            warnings.append("could not count rows (missing pyarrow/openpyxl?)")
        else:
            if rows < spec.get("min_rows", 0):
                problems.append(f"{rows} rows < minimum {spec['min_rows']}")
            past = history.get(name, [])[-7:]
            if past and "max_row_drop_pct" in spec:
                ref = statistics.median(past)
                if ref and rows < ref * (1 - spec["max_row_drop_pct"] / 100):
                    problems.append(f"{rows} rows is {100 * (1 - rows / ref):.0f}% below the recent median {ref:.0f}")
        missing = [c for c in spec.get("required_columns", []) if cols is not None and c not in cols]
        if missing:
            problems.append("missing columns: " + ", ".join(missing))
    if problems:
        return _result(name, FAIL, "; ".join(problems)), rows
    if warnings:
        return _result(name, WARN, "; ".join(warnings)), rows
    detail = f"{age_h:.1f}h old" + (f", {rows:,} rows" if rows is not None else "")
    return _result(name, OK, detail), rows


def check_logs(spec: dict, now: float, base_dir: Path) -> dict:
    name = spec.get("name", "logs")
    pattern = re.compile(spec.get("pattern", r"ERROR|Traceback"))
    since = now - spec.get("hours", 24) * 3600
    hits = []
    pattern_glob = _expand(spec["glob"])
    if not os.path.isabs(pattern_glob):
        pattern_glob = str(base_dir / pattern_glob)   # relative globs are relative to pipeline.toml
    for fn in glob.glob(pattern_glob, recursive=True):
        if os.path.getmtime(fn) < since:
            continue
        with open(fn, "r", encoding="utf-8", errors="replace") as f:
            hits += [f"{Path(fn).name}: {ln.strip()[:100]}" for ln in f if pattern.search(ln)]
    if len(hits) > spec.get("max_matches", 0):
        return _result(name, FAIL, f"{len(hits)} matching line(s); first: {hits[0]}")
    return _result(name, OK, "no error lines in recent logs")


# --------------------------------------------------------------------------- alerting
def notify(webhook: str, text: str, post=None) -> None:
    body = json.dumps({"text": text}).encode("utf-8")
    req = urllib.request.Request(webhook, data=body, headers={"Content-Type": "application/json"})
    (post or urllib.request.urlopen)(req, timeout=15)


def render_markdown(overall: str, results: list[dict], when: str) -> str:
    icon = {OK: "OK", WARN: "WARN", FAIL: "FAIL"}
    lines = [f"# Data status: {icon[overall]}", "", f"Checked at {when}", "", "| Check | Status | Detail |", "|---|---|---|"]
    lines += [f"| {r['check']} | {icon[r['status']]} | {r['detail']} |" for r in results]
    return "\n".join(lines) + "\n"


def run_checks(cfg: dict, base_dir: Path, now: float | None = None, post=None, webhook: str | None = None) -> dict:
    now = time.time() if now is None else now
    p = cfg.get("pipeline", {})
    state_dir = base_dir / p.get("state_dir", "state")
    mon = cfg.get("monitor", {})
    hist_file = state_dir / "row_history.jsonl"
    history = load_history(hist_file)

    results: list[dict] = []
    if mon.get("pipeline_max_age_hours") is not None:
        results.append(check_pipeline(state_dir, float(mon["pipeline_max_age_hours"]), now))
    new_counts: list[tuple[str, int]] = []
    for spec in cfg.get("check", []):
        try:
            if spec.get("type", "file") == "logs":
                results.append(check_logs(spec, now, base_dir))
            else:
                res, rows = check_file(spec, history, now)
                results.append(res)
                if rows is not None and res["status"] == OK:   # only good runs become the baseline
                    new_counts.append((spec.get("name") or spec["path"], rows))
        except ValueError as e:   # e.g. an environment variable used in the path is not set
            results.append(_result(spec.get("name") or spec.get("path") or spec.get("glob", "check"), FAIL, str(e)))

    overall = max((r["status"] for r in results), key=RANK.get, default=OK)
    when = dt.datetime.fromtimestamp(now).isoformat(timespec="seconds")
    report = {"status": overall, "checked_at": when, "results": results}
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "status.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    (state_dir / "status.md").write_text(render_markdown(overall, results, when), encoding="utf-8")
    for n, r in new_counts:
        append_history(hist_file, n, r)

    webhook = webhook or os.environ.get("PIPELINE_ALERT_WEBHOOK")
    if overall != OK and webhook:
        bad = [f"- {r['check']}: {r['detail']}" for r in results if r["status"] != OK]
        try:
            notify(webhook, f"Data status {overall.upper()}\n" + "\n".join(bad), post=post)
            report["alert_sent"] = True
        except Exception as e:  # an alert failure must never hide the real result
            report["alert_sent"] = False
            report["alert_error"] = str(e)
    return report


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=str(Path(__file__).resolve().parents[1] / "pipeline.toml"))
    args = ap.parse_args(argv)
    cfg_path = Path(args.config).resolve()
    with open(cfg_path, "rb") as f:
        cfg = tomllib.load(f)
    report = run_checks(cfg, cfg_path.parent)
    print((cfg_path.parent / cfg.get("pipeline", {}).get("state_dir", "state") / "status.md").read_text(encoding="utf-8"))
    return RANK[report["status"]]


if __name__ == "__main__":
    sys.exit(main())
