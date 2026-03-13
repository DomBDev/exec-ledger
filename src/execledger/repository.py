import sqlite3

from execledger.errors import JobAlreadyExistsError


def add_job(conn: sqlite3.Connection, name: str, command: str) -> None:
    """Insert a job. Raises JobAlreadyExistsError if name already exists."""
    try:
        conn.execute("INSERT INTO jobs (name, command) VALUES (?, ?)", (name, command))
        conn.commit()
    except sqlite3.IntegrityError:
        raise JobAlreadyExistsError(f"job '{name}' already exists") from None
