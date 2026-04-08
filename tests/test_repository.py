import sqlite3
from datetime import datetime, timezone

import pytest

from execledger.db import init_db
from execledger.errors import (
    PipelineAlreadyExistsError,
    PipelineNotFoundError,
    StepAlreadyExistsError,
    StepNotFoundError,
)
from execledger.repository import (
    add_pipeline,
    add_step,
    complete_step_run,
    fail_step_run,
    finish_pipeline_run,
    get_all_pipeline_run_history,
    get_pipeline,
    get_pipeline_run_status,
    get_run_history,
    list_pipelines,
    list_steps,
    remove_pipeline,
    remove_step,
    start_pipeline_run,
    start_step_run,
)


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


def test_get_all_pipeline_run_history() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "a", now)
    add_pipeline(conn, "b", now)
    id_a = start_pipeline_run(conn, "a", now)
    finish_pipeline_run(conn, id_a, now, "completed")
    id_b = start_pipeline_run(conn, "b", now)
    finish_pipeline_run(conn, id_b, now, "failed")
    all_runs = get_all_pipeline_run_history(conn)
    assert len(all_runs) == 2
    assert all_runs[0].pipeline_name == "b"
    assert all_runs[0].status == "failed"
    assert all_runs[1].pipeline_name == "a"
    assert all_runs[1].status == "completed"
    conn.close()


def test_get_pipeline_run_status_not_found() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    with pytest.raises(PipelineNotFoundError):
        get_pipeline_run_status(conn, 999)
    conn.close()
