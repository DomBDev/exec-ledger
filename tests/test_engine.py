import sqlite3
import sys
from datetime import datetime, timezone

import pytest

from execledger.db import init_db
from execledger.engine import resume_pipeline, restart_pipeline, run_pipeline
from execledger.errors import (
    NoResumableRunError,
    PipelineNotFoundError,
    StepConfigurationError,
)
from execledger.repository import (
    add_pipeline,
    add_step,
    get_pipeline_run_status,
    get_run_history,
    start_pipeline_run,
    start_step_run,
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


def test_resume_after_failed_step_completes_remaining(tmp_path) -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "p1", now)
    counter = tmp_path / "n.txt"
    counter.write_text("0")
    script = tmp_path / "bump.py"
    script.write_text(
        "import sys\n"
        "from pathlib import Path\n"
        "p = Path(sys.argv[1])\n"
        "n = int(p.read_text())\n"
        "p.write_text(str(n + 1))\n"
        "raise SystemExit(1 if n == 0 else 0)\n"
    )
    py = sys.executable
    add_step(conn, "p1", "ok", 0, command=f'{py} -c "print(1)"')
    add_step(
        conn,
        "p1",
        "flaky",
        1,
        command=f'"{py}" "{script}" "{counter}"',
    )
    add_step(conn, "p1", "last", 2, command=f'{py} -c "print(3)"')

    run_id = run_pipeline(conn, "p1")
    _, after_fail = get_pipeline_run_status(conn, run_id)
    assert len(after_fail) == 2
    assert after_fail[1].status == "failed"

    resumed_id = resume_pipeline(conn, "p1")
    assert resumed_id == run_id
    _, final = get_pipeline_run_status(conn, run_id)
    assert len(final) == 3
    assert final[0].status == "completed"
    assert final[1].status == "completed"
    assert final[2].step_name == "last"
    assert final[2].status == "completed"
    run, _ = get_pipeline_run_status(conn, run_id)
    assert run.status == "completed"


def test_resume_raises_when_no_pipeline_run_exists() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "p1", now)
    add_step(conn, "p1", "a", 0, command=f'{sys.executable} -c "print(1)"')
    with pytest.raises(NoResumableRunError):
        resume_pipeline(conn, "p1")


def test_resume_raises_when_last_run_succeeded() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "p1", now)
    add_step(conn, "p1", "a", 0, command=f'{sys.executable} -c "print(1)"')
    run_pipeline(conn, "p1")
    with pytest.raises(NoResumableRunError):
        resume_pipeline(conn, "p1")


def test_resume_completes_stuck_active_step() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "p1", now)
    py = sys.executable
    add_step(conn, "p1", "only", 0, command=f'{py} -c "print(99)"')
    run_id = start_pipeline_run(conn, "p1", now)
    start_step_run(conn, run_id, "only", now)
    _, srs = get_pipeline_run_status(conn, run_id)
    assert srs[0].status == "active"

    resume_pipeline(conn, "p1")
    _, after = get_pipeline_run_status(conn, run_id)
    assert after[0].status == "completed"
    run, _ = get_pipeline_run_status(conn, run_id)
    assert run.status == "completed"


def test_restart_pipeline_is_new_run_after_failure() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "p1", now)
    py = sys.executable
    add_step(conn, "p1", "a", 0, command=f'{py} -c "print(1)"')
    add_step(conn, "p1", "b", 1, command=f'{py} -c "import sys; sys.exit(1)"')

    first = run_pipeline(conn, "p1")
    second = restart_pipeline(conn, "p1")
    assert second != first
    hist = get_run_history(conn, "p1")
    assert len(hist) == 2
    assert hist[0].id == second
    assert hist[0].status == "failed"
    assert hist[1].id == first
    assert hist[1].status == "failed"


def test_restart_after_success_creates_second_completed_run() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "p1", now)
    py = sys.executable
    add_step(conn, "p1", "a", 0, command=f'{py} -c "print(1)"')
    first = run_pipeline(conn, "p1")
    second = restart_pipeline(conn, "p1")
    assert second != first
    hist = get_run_history(conn, "p1")
    assert hist[0].status == "completed"
    assert hist[1].status == "completed"
