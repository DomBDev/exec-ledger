import sqlite3

from execledger.db import init_db


def test_jobs_schema() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    cur = conn.execute("PRAGMA table_info(jobs)")
    cols = {row[1] for row in cur.fetchall()}
    assert cols == {"name", "command"}
    conn.close()


def test_runs_schema() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    cur = conn.execute("PRAGMA table_info(runs)")
    cols = {row[1] for row in cur.fetchall()}
    assert cols == {
        "id",
        "job_name",
        "started_at",
        "finished_at",
        "exit_code",
        "stdout",
        "stderr",
    }
    conn.close()


def test_init_db_idempotent() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    init_db(conn)
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cur.fetchall()}
    assert "jobs" in tables and "runs" in tables
    conn.close()
