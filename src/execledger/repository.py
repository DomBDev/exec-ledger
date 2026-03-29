import sqlite3
from datetime import datetime

from execledger.errors import (
    JobAlreadyExistsError,
    JobNotFoundError,
    PipelineAlreadyExistsError,
    PipelineNotFoundError,
)
from execledger.models import Job, Pipeline, RunRecord, Step


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


def get_job(conn: sqlite3.Connection, name: str) -> Job:
    """Return a job by name. Raise JobNotFoundError if the job does not exist."""
    cur = conn.execute(
        "SELECT name, command FROM jobs WHERE name = ?",
        (name,),
    )
    row = cur.fetchone()
    if row is None:
        raise JobNotFoundError(f"job '{name}' not found")
    return Job(name=row[0], command=row[1])


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


def get_all_history(conn: sqlite3.Connection) -> list[RunRecord]:
    """Return all runs, newest first."""
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
        ORDER BY id DESC
        """
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


def add_pipeline(conn: sqlite3.Connection, name: str, created_at: datetime) -> None:
    """Insert a pipeline. Raise PipelineAlreadyExistsError on duplicate."""
    try:
        conn.execute(
            "INSERT INTO pipelines (name, created_at) VALUES (?, ?)",
            (name, created_at.isoformat()),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        raise PipelineAlreadyExistsError(f"pipeline '{name}' already exists") from None


def get_pipeline(conn: sqlite3.Connection, name: str) -> Pipeline:
    """Return a pipeline by name. Raise PipelineNotFoundError if missing."""
    cur = conn.execute(
        "SELECT name, created_at FROM pipelines WHERE name = ?",
        (name,),
    )
    row = cur.fetchone()
    if row is None:
        raise PipelineNotFoundError(f"pipeline '{name}' not found")
    return Pipeline(name=row[0], created_at=datetime.fromisoformat(row[1]))


def list_pipelines(conn: sqlite3.Connection) -> list[Pipeline]:
    """Return all pipelines."""
    cur = conn.execute("SELECT name, created_at FROM pipelines ORDER BY name")
    return [
        Pipeline(name=row[0], created_at=datetime.fromisoformat(row[1]))
        for row in cur.fetchall()
    ]


def remove_pipeline(conn: sqlite3.Connection, name: str) -> None:
    """Delete a pipeline and its steps. Raise PipelineNotFoundError if missing."""
    get_pipeline(conn, name)
    conn.execute("DELETE FROM steps WHERE pipeline_name = ?", (name,))
    conn.execute("DELETE FROM pipelines WHERE name = ?", (name,))
    conn.commit()


def add_step(
    conn: sqlite3.Connection,
    pipeline_name: str,
    name: str,
    position: int,
    command: str | None = None,
    func_ref: str | None = None,
) -> None:
    """Add a step to a pipeline. Pipeline must exist."""
    get_pipeline(conn, pipeline_name)
    conn.execute(
        """
        INSERT INTO steps (pipeline_name, name, command, func_ref, position)
        VALUES (?, ?, ?, ?, ?)
        """,
        (pipeline_name, name, command, func_ref, position),
    )
    conn.commit()


def list_steps(conn: sqlite3.Connection, pipeline_name: str) -> list[Step]:
    """Return steps for a pipeline, ordered by position."""
    cur = conn.execute(
        """
        SELECT pipeline_name, name, command, func_ref, position
        FROM steps
        WHERE pipeline_name = ?
        ORDER BY position
        """,
        (pipeline_name,),
    )
    return [
        Step(
            pipeline_name=row[0],
            name=row[1],
            command=row[2],
            func_ref=row[3],
            position=row[4],
        )
        for row in cur.fetchall()
    ]


def remove_step(conn: sqlite3.Connection, pipeline_name: str, step_name: str) -> None:
    """Remove a step from a pipeline."""
    cur = conn.execute(
        "DELETE FROM steps WHERE pipeline_name = ? AND name = ?",
        (pipeline_name, step_name),
    )
    if cur.rowcount == 0:
        raise PipelineNotFoundError(
            f"step '{step_name}' not found in pipeline '{pipeline_name}'"
        )
    conn.commit()
