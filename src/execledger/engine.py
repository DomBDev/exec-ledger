import sqlite3
from datetime import datetime, timezone

from execledger.errors import (
    ExecutionError,
    NoResumableRunError,
    StepConfigurationError,
)
from execledger.repository import (
    complete_step_run,
    fail_step_run,
    finish_pipeline_run,
    get_latest_resumable_run_id,
    get_pipeline,
    get_pipeline_run_status,
    list_steps,
    reopen_pipeline_run,
    reopen_step_run,
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


def restart_pipeline(conn: sqlite3.Connection, pipeline_name: str) -> int:
    """Start a new run from step 1. Prior runs stay in history."""
    return run_pipeline(conn, pipeline_name)


def resume_pipeline(conn: sqlite3.Connection, pipeline_name: str) -> int:
    """Continue the latest failed or interrupted run from the first failed/active step."""
    get_pipeline(conn, pipeline_name)
    steps = list_steps(conn, pipeline_name)
    for step in steps:
        _step_kind(step.command, step.func_ref)

    run_id = get_latest_resumable_run_id(conn, pipeline_name)
    _, existing = get_pipeline_run_status(conn, run_id)
    by_name = {sr.step_name: sr for sr in existing}

    resume_idx: int | None = None
    for i, step in enumerate(steps):
        sr = by_name.get(step.name)
        if sr and sr.status in ("failed", "active"):
            resume_idx = i
            break

    if resume_idx is None:
        raise NoResumableRunError(f"no failed or active step to resume in run {run_id}")

    reopen_pipeline_run(conn, run_id)

    for i in range(resume_idx, len(steps)):
        step = steps[i]
        kind, payload = _step_kind(step.command, step.func_ref)
        t0 = datetime.now(timezone.utc)
        if i == resume_idx:
            sr = by_name[step.name]
            step_run_id = sr.id
            if step_run_id is None:
                raise NoResumableRunError("step run has no id")
            reopen_step_run(conn, step_run_id, t0)
        else:
            step_run_id = start_step_run(conn, run_id, step.name, t0)
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
