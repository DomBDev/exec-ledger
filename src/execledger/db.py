from pathlib import Path

import sqlite3


def get_db_path() -> Path:
    """Path to .execledger/execledger.db (project local)."""
    return Path(".execledger") / "execledger.db"


def init_db(conn: sqlite3.Connection) -> None:
    """Create all tables if they do not exist."""
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (
            name TEXT PRIMARY KEY,
            command TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            exit_code INTEGER NOT NULL,
            stdout TEXT NOT NULL,
            stderr TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS pipelines (
            name TEXT PRIMARY KEY,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS steps (
            pipeline_name TEXT NOT NULL,
            name TEXT NOT NULL,
            command TEXT,
            func_ref TEXT,
            position INTEGER NOT NULL,
            PRIMARY KEY (pipeline_name, name),
            FOREIGN KEY (pipeline_name) REFERENCES pipelines(name)
        );
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_name TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            status TEXT NOT NULL,
            FOREIGN KEY (pipeline_name) REFERENCES pipelines(name)
        );
        CREATE TABLE IF NOT EXISTS step_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            step_name TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            exit_code INTEGER,
            stdout TEXT NOT NULL DEFAULT '',
            stderr TEXT NOT NULL DEFAULT '',
            FOREIGN KEY (run_id) REFERENCES pipeline_runs(id)
        );
    """)


def get_connection() -> sqlite3.Connection:
    """Open connection, ensure schema exists. Caller must close."""
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    init_db(conn)
    return conn
