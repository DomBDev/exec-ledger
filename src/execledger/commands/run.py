from datetime import datetime, timezone

import typer

from execledger.db import get_connection
from execledger.errors import ExecutionError, JobNotFoundError
from execledger.models import RunRecord
from execledger.repository import add_run, get_job
from execledger.runner import run_job


def run(name: str) -> None:
    """Run a job by name."""
    conn = get_connection()
    try:
        job = get_job(conn, name)
    except JobNotFoundError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1) from e
    finally:
        conn.close()

    started_at = datetime.now(timezone.utc)
    try:
        exit_code, stdout, stderr = run_job(job.command)
    except ExecutionError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1) from e

    finished_at = datetime.now(timezone.utc)
    record = RunRecord(
        job_name=job.name,
        started_at=started_at,
        finished_at=finished_at,
        exit_code=exit_code,
        stdout=stdout,
        stderr=stderr,
    )

    conn = get_connection()
    try:
        add_run(conn, record)
    except JobNotFoundError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1) from e
    except Exception as e:
        typer.echo(f"Failed to save run record: {e}", err=True)
        raise typer.Exit(1) from e
    finally:
        conn.close()

    if stdout:
        typer.echo(stdout, nl=False)
    if stderr:
        typer.echo(stderr, err=True, nl=False)
    raise typer.Exit(exit_code)
