import sqlite3
from datetime import datetime, timezone

from execledger.errors import ExecutionError, StepConfigurationError
from execledger.repository import (
    complete_step_run,
    fail_step_run,
    finish_pipeline_run,
    list_steps,
    start_pipeline_run,
    start_step_run,
)
from execledger.runner import run_command, run_function


def _step_kind(step_command: str | None, step_func_ref: str | None) -> tuple[str, str]:
    cmd = (step_command or "").strip()
    fn = (step_func_ref or "").strip()
    has_cmd = bool(cmd)
    has_fn = bool(fn)
    if has_cmd and has_fn:
        raise StepConfigurationError("step cannot set both command and func_ref")
    if not has_cmd and not has_fn:
        raise StepConfigurationError("step needs command or func_ref")
    if has_cmd:
        return "command", cmd
    return "func", fn


def _execute_step(kind: str, payload: str) -> tuple[int, str, str]:
    if kind == "command":
        try:
            return run_command(payload)
        except ExecutionError as e:
            return 1, "", str(e)
    try:
        return run_function(payload)
    except ExecutionError as e:
        return 1, "", str(e)


def run_pipeline(conn: sqlite3.Connection, pipeline_name: str) -> int:
    """Run every step in order for one new pipeline run. Returns run id.

    Validates steps before inserting a pipeline run. Then for each step:
    step_run active -> completed or failed. Stops on first non-zero exit.
    On full success, marks the pipeline run completed.
    """
    steps = list_steps(conn, pipeline_name)
    planned: list[tuple[str, str, str]] = []
    for step in steps:
        kind, payload = _step_kind(step.command, step.func_ref)
        planned.append((step.name, kind, payload))

    now = datetime.now(timezone.utc)
    run_id = start_pipeline_run(conn, pipeline_name, now)

    for step_name, kind, payload in planned:
        t0 = datetime.now(timezone.utc)
        step_run_id = start_step_run(conn, run_id, step_name, t0)
        exit_code, stdout, stderr = _execute_step(kind, payload)
        t1 = datetime.now(timezone.utc)
        if exit_code == 0:
            complete_step_run(conn, step_run_id, t1, exit_code, stdout, stderr)
        else:
            fail_step_run(conn, step_run_id, t1, exit_code, stdout, stderr)
            finish_pipeline_run(conn, run_id, t1, "failed")
            return run_id

    finish_pipeline_run(conn, run_id, datetime.now(timezone.utc), "completed")
    return run_id
