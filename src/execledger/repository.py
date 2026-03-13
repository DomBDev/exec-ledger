import sqlite3
from datetime import datetime

from execledger.errors import JobAlreadyExistsError, JobNotFoundError
from execledger.models import Job, RunRecord


def add_job(conn: sqlite3.Connection, name: str, command: str) -> None:
    """Insert a job. Raise JobAlreadyExistsError if the name already exists."""
    try:
        conn.execute(
            "INSERT INTO jobs (name, command) VALUES (?, ?)",
            (name, command),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        raise JobAlreadyExistsError(f"job '{name}' already exists") from None


def list_jobs(conn: sqlite3.Connection) -> list[Job]:
    """Return all jobs."""
    cur = conn.execute("SELECT name, command FROM jobs ORDER BY name")
    return [Job(name=row[0], command=row[1]) for row in cur.fetchall()]


def remove_job(conn: sqlite3.Connection, name: str) -> None:
    """Delete a job. Raise JobNotFoundError if the job does not exist."""
    cur = conn.execute("DELETE FROM jobs WHERE name = ?", (name,))
    if cur.rowcount == 0:
        raise JobNotFoundError(f"job '{name}' not found")
    conn.commit()


def add_run(conn: sqlite3.Connection, record: RunRecord) -> None:
    """Store a run record. Raise JobNotFoundError if the job does not exist."""
    cur = conn.execute("SELECT 1 FROM jobs WHERE name = ?", (record.job_name,))
    if cur.fetchone() is None:
        raise JobNotFoundError(f"job '{record.job_name}' not found")
    conn.execute(
        """
        INSERT INTO runs (
            job_name,
            started_at,
            finished_at,
            exit_code,
            stdout,
            stderr
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            record.job_name,
            record.started_at.isoformat(),
            record.finished_at.isoformat(),
            record.exit_code,
            record.stdout,
            record.stderr,
        ),
    )
    conn.commit()


def get_history(conn: sqlite3.Connection, job_name: str) -> list[RunRecord]:
    """Return runs for a job, newest first."""
    cur = conn.execute(
        """
        SELECT
            job_name,
            started_at,
            finished_at,
            exit_code,
            stdout,
            stderr
        FROM runs
        WHERE job_name = ?
        ORDER BY id DESC
        """,
        (job_name,),
    )
    return [
        RunRecord(
            job_name=row[0],
            started_at=datetime.fromisoformat(row[1]),
            finished_at=datetime.fromisoformat(row[2]),
            exit_code=row[3],
            stdout=row[4],
            stderr=row[5],
        )
        for row in cur.fetchall()
    ]
