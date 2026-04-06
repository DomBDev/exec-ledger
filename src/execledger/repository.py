import sqlite3
from datetime import datetime

from execledger.errors import (
    NoResumableRunError,
    PipelineAlreadyExistsError,
    PipelineNotFoundError,
    StepAlreadyExistsError,
    StepNotFoundError,
)
from execledger.models import Pipeline, PipelineRun, Step, StepRun


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
    try:
        conn.execute(
            """
            INSERT INTO steps (pipeline_name, name, command, func_ref, position)
            VALUES (?, ?, ?, ?, ?)
            """,
            (pipeline_name, name, command, func_ref, position),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        conn.rollback()
        raise StepAlreadyExistsError(
            f"step '{name}' already exists in pipeline '{pipeline_name}'"
        ) from None


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
    """Remove a step. Raise PipelineNotFoundError or StepNotFoundError."""
    get_pipeline(conn, pipeline_name)
    cur = conn.execute(
        "DELETE FROM steps WHERE pipeline_name = ? AND name = ?",
        (pipeline_name, step_name),
    )
    if cur.rowcount == 0:
        raise StepNotFoundError(
            f"step '{step_name}' not found in pipeline '{pipeline_name}'"
        )
    conn.commit()


def start_pipeline_run(
    conn: sqlite3.Connection, pipeline_name: str, started_at: datetime
) -> int:
    """Create a pipeline run. Returns the run ID."""
    get_pipeline(conn, pipeline_name)
    cur = conn.execute(
        "INSERT INTO pipeline_runs (pipeline_name, started_at, status) VALUES (?, ?, ?)",
        (pipeline_name, started_at.isoformat(), "running"),
    )
    conn.commit()
    assert cur.lastrowid is not None
    return cur.lastrowid


def start_step_run(
    conn: sqlite3.Connection, run_id: int, step_name: str, started_at: datetime
) -> int:
    """Mark a step as active. Returns the step_run ID."""
    cur = conn.execute(
        "INSERT INTO step_runs (run_id, step_name, status, started_at) VALUES (?, ?, ?, ?)",
        (run_id, step_name, "active", started_at.isoformat()),
    )
    conn.commit()
    assert cur.lastrowid is not None
    return cur.lastrowid


def complete_step_run(
    conn: sqlite3.Connection,
    step_run_id: int,
    finished_at: datetime,
    exit_code: int,
    stdout: str,
    stderr: str,
) -> None:
    """Mark a step as completed with its output."""
    conn.execute(
        """
        UPDATE step_runs
        SET status = ?, finished_at = ?, exit_code = ?, stdout = ?, stderr = ?
        WHERE id = ?
        """,
        ("completed", finished_at.isoformat(), exit_code, stdout, stderr, step_run_id),
    )
    conn.commit()


def fail_step_run(
    conn: sqlite3.Connection,
    step_run_id: int,
    finished_at: datetime,
    exit_code: int,
    stdout: str,
    stderr: str,
) -> None:
    """Mark a step as failed with its output."""
    conn.execute(
        """
        UPDATE step_runs
        SET status = ?, finished_at = ?, exit_code = ?, stdout = ?, stderr = ?
        WHERE id = ?
        """,
        ("failed", finished_at.isoformat(), exit_code, stdout, stderr, step_run_id),
    )
    conn.commit()


def finish_pipeline_run(
    conn: sqlite3.Connection, run_id: int, finished_at: datetime, status: str
) -> None:
    """Mark a pipeline run as completed or failed."""
    conn.execute(
        "UPDATE pipeline_runs SET finished_at = ?, status = ? WHERE id = ?",
        (finished_at.isoformat(), status, run_id),
    )
    conn.commit()


def get_latest_resumable_run_id(conn: sqlite3.Connection, pipeline_name: str) -> int:
    """Latest pipeline run that is failed or still running. For resume."""
    row = conn.execute(
        """
        SELECT id FROM pipeline_runs
        WHERE pipeline_name = ? AND status IN ('failed', 'running')
        ORDER BY id DESC
        LIMIT 1
        """,
        (pipeline_name,),
    ).fetchone()
    if row is None:
        raise NoResumableRunError(f"no resumable run for pipeline '{pipeline_name}'")
    return int(row[0])


def reopen_pipeline_run(conn: sqlite3.Connection, run_id: int) -> None:
    """Clear finished_at and set status to running before continuing a run."""
    conn.execute(
        "UPDATE pipeline_runs SET status = ?, finished_at = NULL WHERE id = ?",
        ("running", run_id),
    )
    conn.commit()


def reopen_step_run(
    conn: sqlite3.Connection, step_run_id: int, started_at: datetime
) -> None:
    """Reset a step row to active so the step can be executed again."""
    conn.execute(
        """
        UPDATE step_runs
        SET status = ?, started_at = ?, finished_at = NULL,
            exit_code = NULL, stdout = '', stderr = ''
        WHERE id = ?
        """,
        ("active", started_at.isoformat(), step_run_id),
    )
    conn.commit()


def get_pipeline_run_status(
    conn: sqlite3.Connection, run_id: int
) -> tuple[PipelineRun, list[StepRun]]:
    """Return a pipeline run and its step runs."""
    row = conn.execute(
        "SELECT id, pipeline_name, started_at, finished_at, status FROM pipeline_runs WHERE id = ?",
        (run_id,),
    ).fetchone()
    if row is None:
        raise PipelineNotFoundError(f"run {run_id} not found")
    run = PipelineRun(
        id=row[0],
        pipeline_name=row[1],
        started_at=datetime.fromisoformat(row[2]) if row[2] else None,
        finished_at=datetime.fromisoformat(row[3]) if row[3] else None,
        status=row[4],
    )
    step_rows = conn.execute(
        """
        SELECT id, run_id, step_name, status, started_at, finished_at, exit_code, stdout, stderr
        FROM step_runs WHERE run_id = ?
        ORDER BY id
        """,
        (run_id,),
    ).fetchall()
    step_runs = [
        StepRun(
            id=r[0],
            run_id=r[1],
            step_name=r[2],
            status=r[3],
            started_at=datetime.fromisoformat(r[4]) if r[4] else None,
            finished_at=datetime.fromisoformat(r[5]) if r[5] else None,
            exit_code=r[6],
            stdout=r[7],
            stderr=r[8],
        )
        for r in step_rows
    ]
    return run, step_runs


def get_run_history(conn: sqlite3.Connection, pipeline_name: str) -> list[PipelineRun]:
    """Return past runs for a pipeline, newest first."""
    cur = conn.execute(
        """
        SELECT id, pipeline_name, started_at, finished_at, status
        FROM pipeline_runs
        WHERE pipeline_name = ?
        ORDER BY id DESC
        """,
        (pipeline_name,),
    )
    return [
        PipelineRun(
            id=row[0],
            pipeline_name=row[1],
            started_at=datetime.fromisoformat(row[2]) if row[2] else None,
            finished_at=datetime.fromisoformat(row[3]) if row[3] else None,
            status=row[4],
        )
        for row in cur.fetchall()
    ]


def get_all_pipeline_run_history(conn: sqlite3.Connection) -> list[PipelineRun]:
    """Return all pipeline runs across names, newest first."""
    cur = conn.execute(
        """
        SELECT id, pipeline_name, started_at, finished_at, status
        FROM pipeline_runs
        ORDER BY id DESC
        """
    )
    return [
        PipelineRun(
            id=row[0],
            pipeline_name=row[1],
            started_at=datetime.fromisoformat(row[2]) if row[2] else None,
            finished_at=datetime.fromisoformat(row[3]) if row[3] else None,
            status=row[4],
        )
        for row in cur.fetchall()
    ]
