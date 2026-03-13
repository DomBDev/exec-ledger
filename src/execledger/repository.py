import sqlite3

from execledger.errors import JobAlreadyExistsError, JobNotFoundError
from execledger.models import Job


def add_job(conn: sqlite3.Connection, name: str, command: str) -> None:
    """Insert a job. Raises JobAlreadyExistsError if name already exists."""
    try:
        conn.execute("INSERT INTO jobs (name, command) VALUES (?, ?)", (name, command))
        conn.commit()
    except sqlite3.IntegrityError:
        raise JobAlreadyExistsError(f"job '{name}' already exists") from None


def list_jobs(conn: sqlite3.Connection) -> list[Job]:
    """Return all jobs."""
    cur = conn.execute("SELECT name, command FROM jobs ORDER BY name")
    return [Job(name=row[0], command=row[1]) for row in cur.fetchall()]


def remove_job(conn: sqlite3.Connection, name: str) -> None:
    """Delete a job. Raises JobNotFoundError if name does not exist."""
    cur = conn.execute("DELETE FROM jobs WHERE name = ?", (name,))
    if cur.rowcount == 0:
        raise JobNotFoundError(f"job '{name}' not found")
    conn.commit()
