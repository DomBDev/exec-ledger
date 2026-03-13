from pathlib import Path

import sqlite3


def get_db_path() -> Path:
    """Path to .execledger/execledger.db (project local)."""
    return Path(".execledger") / "execledger.db"


def init_db(conn: sqlite3.Connection) -> None:
    """Create jobs and runs tables if they do not exist."""
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
    """)
