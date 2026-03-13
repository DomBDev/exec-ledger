import sqlite3
from datetime import datetime, timezone

import pytest

from execledger.db import init_db
from execledger.errors import JobAlreadyExistsError, JobNotFoundError
from execledger.models import Job, RunRecord
from execledger.repository import (
    add_job,
    add_run,
    get_history,
    get_job,
    list_jobs,
    remove_job,
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


def test_add_job_duplicate_raises() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    add_job(conn, "backup", "echo done")
    with pytest.raises(JobAlreadyExistsError):
        add_job(conn, "backup", "echo other")
    conn.close()
