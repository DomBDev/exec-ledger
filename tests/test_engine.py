import sqlite3
import sys
from datetime import datetime, timezone

import pytest

from execledger.db import init_db
from execledger.engine import run_pipeline
from execledger.errors import PipelineNotFoundError, StepConfigurationError
from execledger.repository import (
    add_pipeline,
    add_step,
    get_pipeline_run_status,
)


def test_run_pipeline_two_command_steps_success() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "p1", now)
    py = sys.executable
    add_step(conn, "p1", "a", 0, command=f"{py} -c \"print('a')\"")
    add_step(conn, "p1", "b", 1, command=f"{py} -c \"print('b')\"")

    run_id = run_pipeline(conn, "p1")
    run, step_runs = get_pipeline_run_status(conn, run_id)
    assert run.status == "completed"
    assert len(step_runs) == 2
    assert step_runs[0].step_name == "a"
    assert step_runs[0].status == "completed"
    assert step_runs[1].step_name == "b"
    assert step_runs[1].status == "completed"


def test_run_pipeline_stops_after_first_failing_step() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "p1", now)
    py = sys.executable
    add_step(conn, "p1", "ok", 0, command=f'{py} -c "print(1)"')
    add_step(conn, "p1", "bad", 1, command=f'{py} -c "import sys; sys.exit(2)"')
    add_step(conn, "p1", "skip", 2, command=f'{py} -c "print(3)"')

    run_id = run_pipeline(conn, "p1")
    run, step_runs = get_pipeline_run_status(conn, run_id)
    assert run.status == "failed"
    assert len(step_runs) == 2
    assert step_runs[0].status == "completed"
    assert step_runs[1].status == "failed"
    assert step_runs[1].exit_code == 2


def test_run_pipeline_function_step() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "p1", now)
    add_step(conn, "p1", "fn", 0, func_ref="os:getcwd")

    run_id = run_pipeline(conn, "p1")
    run, step_runs = get_pipeline_run_status(conn, run_id)
    assert run.status == "completed"
    assert step_runs[0].status == "completed"
    assert step_runs[0].exit_code == 0


def test_run_pipeline_empty_completes() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "empty", now)

    run_id = run_pipeline(conn, "empty")
    run, step_runs = get_pipeline_run_status(conn, run_id)
    assert run.status == "completed"
    assert step_runs == []


def test_run_pipeline_invalid_step_before_run_raises() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "p1", now)
    add_step(conn, "p1", "bad", 0)

    with pytest.raises(StepConfigurationError):
        run_pipeline(conn, "p1")


def test_run_pipeline_command_execution_error_recorded() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "p1", now)
    add_step(conn, "p1", "badcmd", 0, command="nonexistent_command_xyz_12345")

    run_id = run_pipeline(conn, "p1")
    run, step_runs = get_pipeline_run_status(conn, run_id)
    assert run.status == "failed"
    assert step_runs[0].status == "failed"
    assert step_runs[0].exit_code == 1
    assert step_runs[0].stderr != ""


def test_run_pipeline_missing_pipeline() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    with pytest.raises(PipelineNotFoundError):
        run_pipeline(conn, "nope")
