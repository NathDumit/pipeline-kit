# pipeline-kit

A small, dependency-light **orchestrator**, **data monitor** and **test suite** for daily data pipelines.
Pure Python standard library for the runner and monitor; `pytest` for the tests. Everything in this repository runs on synthetic data.

It exists because a script that "ran without error" is not the same as data you can trust.

## What it does

**Orchestrator** (`orchestrator/run_pipeline.py`)
- Runs steps from `pipeline.toml` in dependency order (topological sort).
- Retries with exponential backoff, enforces a timeout per step, and writes one log per step.
- Skips steps whose dependencies failed, and can restrict a step to certain weekdays.
- Uses a lock file so two runs never overlap, with stale-lock detection.
- Per-step environment variables with strict `${VAR}` expansion: an undefined variable stops the run before any step starts, instead of silently becoming literal text.
- Exit codes: `0` ok, `1` a step failed, `2` bad configuration, `3` another run is active.

**Monitor** (`monitoring/monitor.py`)
- Answers one question: *can I trust today's outputs?*
- Checks the last run, file freshness, minimum size and rows, required columns (schema drift), a silent row-count drop against the median of previous good runs, and error lines in recent logs.
- Writes `state/status.md` and `state/status.json`, and can post to a webhook (`PIPELINE_ALERT_WEBHOOK`) only when something is wrong.

## Try it

```bash
pip install -r requirements-dev.txt
python -m pytest tests -q

PIPELINE_OUT=demo_out python orchestrator/run_pipeline.py --dry-run   # show the plan
PIPELINE_OUT=demo_out python orchestrator/run_pipeline.py             # run the demo
PIPELINE_OUT=demo_out python monitoring/monitor.py                    # verify the outputs
```

On Windows `cmd`, use `set PIPELINE_OUT=demo_out` before the commands.

## Lessons that shaped the design

These came from running a real pipeline for the first time:
1. **The incremental mode needs an initial full load.** Make the bootstrap an explicit step.
2. **An unset environment variable must be an error.** It used to be passed through as text and wrote files to the wrong place.
3. **Scripts with hard-coded paths ignore the orchestrator.** The monitor caught this, because the expected output file was missing.
4. **Tests go stale when the logic changes.** That is the point of having them: the failing test shows what to update.

## Not covered

- It is not a replacement for Airflow, Prefect or Dagster when you need a scheduler UI, distributed workers or backfills. It is a deliberately small tool for a single machine.
- Scheduling itself (cron or Windows Task Scheduler) is left to the operating system.
- The monitor's row-drop check needs a history of good runs before it can judge anything.

## License

MIT, see [LICENSE](LICENSE).
