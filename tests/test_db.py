import sqlite3

from execledger.db import init_db


def test_pipelines_schema() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    cur = conn.execute("PRAGMA table_info(pipelines)")
    cols = {row[1] for row in cur.fetchall()}
    assert cols == {"name", "created_at"}
    conn.close()


def test_steps_schema() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    cur = conn.execute("PRAGMA table_info(steps)")
    cols = {row[1] for row in cur.fetchall()}
    assert cols == {"pipeline_name", "name", "command", "func_ref", "position"}
    conn.close()


def test_pipeline_runs_schema() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    cur = conn.execute("PRAGMA table_info(pipeline_runs)")
    cols = {row[1] for row in cur.fetchall()}
    assert cols == {"id", "pipeline_name", "started_at", "finished_at", "status"}
    conn.close()


def test_step_runs_schema() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    cur = conn.execute("PRAGMA table_info(step_runs)")
    cols = {row[1] for row in cur.fetchall()}
    assert cols == {
        "id",
        "run_id",
        "step_name",
        "status",
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
    assert "pipelines" in tables
    assert "steps" in tables
    assert "pipeline_runs" in tables
    assert "step_runs" in tables
    conn.close()
