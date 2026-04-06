import sqlite3
from datetime import datetime, timezone

import pytest

from execledger.db import init_db
from execledger.errors import (
    JobAlreadyExistsError,
    JobNotFoundError,
    PipelineAlreadyExistsError,
    PipelineNotFoundError,
    StepAlreadyExistsError,
    StepNotFoundError,
)
from execledger.models import Job, RunRecord
from execledger.repository import (
    add_job,
    add_pipeline,
    add_run,
    add_step,
    complete_step_run,
    fail_step_run,
    finish_pipeline_run,
    get_all_history,
    get_history,
    get_job,
    get_pipeline,
    get_pipeline_run_status,
    get_run_history,
    list_jobs,
    list_pipelines,
    list_steps,
    remove_job,
    remove_pipeline,
    remove_step,
    start_pipeline_run,
    start_step_run,
)


def test_add_job() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    add_job(conn, "backup", "echo done")
    row = conn.execute(
        "SELECT name, command FROM jobs WHERE name = ?",
        ("backup",),
    ).fetchone()
    assert row == ("backup", "echo done")
    conn.close()


def test_list_jobs() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    add_job(conn, "backup", "echo done")
    add_job(conn, "deploy", "echo deploy")
    jobs = list_jobs(conn)
    assert jobs == [
        Job(name="backup", command="echo done"),
        Job(name="deploy", command="echo deploy"),
    ]
    conn.close()


def test_list_jobs_empty() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    assert list_jobs(conn) == []
    conn.close()


def test_remove_job() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    add_job(conn, "backup", "echo done")
    remove_job(conn, "backup")
    assert list_jobs(conn) == []
    conn.close()


def test_remove_job_not_found_raises() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    with pytest.raises(JobNotFoundError):
        remove_job(conn, "nonexistent")
    conn.close()


def test_get_job() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    add_job(conn, "backup", "echo done")
    job = get_job(conn, "backup")
    assert job == Job(name="backup", command="echo done")
    conn.close()


def test_get_job_not_found_raises() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    with pytest.raises(JobNotFoundError):
        get_job(conn, "nonexistent")
    conn.close()


def test_add_run() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    add_job(conn, "backup", "echo done")
    now = datetime.now(timezone.utc)
    record = RunRecord(
        job_name="backup",
        started_at=now,
        finished_at=now,
        exit_code=0,
        stdout="done",
        stderr="",
    )
    add_run(conn, record)
    row = conn.execute(
        "SELECT job_name, started_at, finished_at, exit_code, stdout, stderr FROM runs WHERE job_name = ?",
        ("backup",),
    ).fetchone()
    assert row == (
        "backup",
        now.isoformat(),
        now.isoformat(),
        0,
        "done",
        "",
    )
    conn.close()


def test_get_history() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    add_job(conn, "backup", "echo done")
    t1 = datetime.now(timezone.utc)
    t2 = datetime.now(timezone.utc)
    add_run(conn, RunRecord("backup", t1, t1, 0, "first", ""))
    add_run(conn, RunRecord("backup", t2, t2, 1, "second", "err"))
    history = get_history(conn, "backup")
    assert len(history) == 2
    assert history[0].exit_code == 1
    assert history[0].stdout == "second"
    assert history[1].exit_code == 0
    assert history[1].stdout == "first"
    conn.close()


def test_add_run_job_not_found_raises() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    record = RunRecord("nonexistent", now, now, 0, "", "")
    with pytest.raises(JobNotFoundError):
        add_run(conn, record)
    conn.close()


def test_get_history_empty() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    assert get_history(conn, "nonexistent") == []
    conn.close()


def test_get_all_history() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    add_job(conn, "a", "echo a")
    add_job(conn, "b", "echo b")
    now = datetime.now(timezone.utc)
    add_run(conn, RunRecord("a", now, now, 0, "", ""))
    add_run(conn, RunRecord("b", now, now, 1, "", ""))
    runs = get_all_history(conn)
    assert len(runs) == 2
    assert {r.job_name for r in runs} == {"a", "b"}
    conn.close()


def test_add_job_duplicate_raises() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    add_job(conn, "backup", "echo done")
    with pytest.raises(JobAlreadyExistsError):
        add_job(conn, "backup", "echo other")
    conn.close()


def test_add_pipeline() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "deploy", now)
    p = get_pipeline(conn, "deploy")
    assert p.name == "deploy"
    conn.close()


def test_add_pipeline_duplicate_raises() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "deploy", now)
    with pytest.raises(PipelineAlreadyExistsError):
        add_pipeline(conn, "deploy", now)
    conn.close()


def test_get_pipeline_not_found_raises() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    with pytest.raises(PipelineNotFoundError):
        get_pipeline(conn, "nonexistent")
    conn.close()


def test_list_pipelines() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "a", now)
    add_pipeline(conn, "b", now)
    pipes = list_pipelines(conn)
    assert len(pipes) == 2
    assert pipes[0].name == "a"
    assert pipes[1].name == "b"
    conn.close()


def test_list_pipelines_empty() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    assert list_pipelines(conn) == []
    conn.close()


def test_remove_pipeline() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "deploy", now)
    remove_pipeline(conn, "deploy")
    assert list_pipelines(conn) == []
    conn.close()


def test_remove_pipeline_not_found_raises() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    with pytest.raises(PipelineNotFoundError):
        remove_pipeline(conn, "nonexistent")
    conn.close()


def test_remove_pipeline_deletes_steps() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "deploy", now)
    add_step(conn, "deploy", "build", 0, command="make build")
    add_step(conn, "deploy", "test", 1, command="make test")
    remove_pipeline(conn, "deploy")
    assert list_steps(conn, "deploy") == []
    conn.close()


def test_add_step() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "deploy", now)
    add_step(conn, "deploy", "build", 0, command="make build")
    steps = list_steps(conn, "deploy")
    assert len(steps) == 1
    assert steps[0].name == "build"
    assert steps[0].command == "make build"
    assert steps[0].position == 0
    conn.close()


def test_add_step_pipeline_not_found_raises() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    with pytest.raises(PipelineNotFoundError):
        add_step(conn, "nonexistent", "build", 0, command="make build")
    conn.close()


def test_add_step_duplicate_raises() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "deploy", now)
    add_step(conn, "deploy", "build", 0, command="make build")
    with pytest.raises(StepAlreadyExistsError):
        add_step(conn, "deploy", "build", 0, command="make test")
    conn.close()


def test_list_steps_ordered_by_position() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "deploy", now)
    add_step(conn, "deploy", "test", 1, command="make test")
    add_step(conn, "deploy", "build", 0, command="make build")
    steps = list_steps(conn, "deploy")
    assert steps[0].name == "build"
    assert steps[1].name == "test"
    conn.close()


def test_list_steps_empty() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    assert list_steps(conn, "nonexistent") == []
    conn.close()


def test_remove_step() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "deploy", now)
    add_step(conn, "deploy", "build", 0, command="make build")
    remove_step(conn, "deploy", "build")
    assert list_steps(conn, "deploy") == []
    conn.close()


def test_remove_step_not_found_raises() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "deploy", now)
    with pytest.raises(StepNotFoundError):
        remove_step(conn, "deploy", "nonexistent")
    conn.close()


def test_remove_step_pipeline_not_found_raises() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    with pytest.raises(PipelineNotFoundError):
        remove_step(conn, "deploy", "build")
    conn.close()


def test_start_pipeline_run() -> None:
    """start_pipeline_run creates a run with status 'running' and returns its ID."""
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "deploy", now)
    run_id = start_pipeline_run(conn, "deploy", now)
    assert isinstance(run_id, int)
    run, _ = get_pipeline_run_status(conn, run_id)
    assert run.status == "running"
    assert run.pipeline_name == "deploy"
    conn.close()


def test_step_run_lifecycle() -> None:
    """A step goes from active to completed with output stored."""
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "deploy", now)
    run_id = start_pipeline_run(conn, "deploy", now)
    sr_id = start_step_run(conn, run_id, "build", now)
    complete_step_run(conn, sr_id, now, 0, "built ok", "")
    _, step_runs = get_pipeline_run_status(conn, run_id)
    assert len(step_runs) == 1
    assert step_runs[0].status == "completed"
    assert step_runs[0].exit_code == 0
    assert step_runs[0].stdout == "built ok"
    conn.close()


def test_step_run_failure() -> None:
    """A failed step stores exit code and stderr."""
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "deploy", now)
    run_id = start_pipeline_run(conn, "deploy", now)
    sr_id = start_step_run(conn, run_id, "test", now)
    fail_step_run(conn, sr_id, now, 1, "", "tests failed")
    _, step_runs = get_pipeline_run_status(conn, run_id)
    assert step_runs[0].status == "failed"
    assert step_runs[0].exit_code == 1
    assert step_runs[0].stderr == "tests failed"
    conn.close()


def test_finish_pipeline_run() -> None:
    """finish_pipeline_run sets the final status and timestamp."""
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "deploy", now)
    run_id = start_pipeline_run(conn, "deploy", now)
    finish_pipeline_run(conn, run_id, now, "completed")
    run, _ = get_pipeline_run_status(conn, run_id)
    assert run.status == "completed"
    assert run.finished_at is not None
    conn.close()


def test_get_run_history() -> None:
    """get_run_history returns runs newest first."""
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "deploy", now)
    id1 = start_pipeline_run(conn, "deploy", now)
    finish_pipeline_run(conn, id1, now, "completed")
    id2 = start_pipeline_run(conn, "deploy", now)
    finish_pipeline_run(conn, id2, now, "failed")
    history = get_run_history(conn, "deploy")
    assert len(history) == 2
    assert history[0].id == id2
    assert history[0].status == "failed"
    assert history[1].id == id1
    assert history[1].status == "completed"
    conn.close()


def test_get_pipeline_run_status_not_found() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    with pytest.raises(PipelineNotFoundError):
        get_pipeline_run_status(conn, 999)
    conn.close()
