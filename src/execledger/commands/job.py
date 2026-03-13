import typer

from execledger.db import get_connection
from execledger.errors import JobAlreadyExistsError, JobNotFoundError
from execledger.repository import add_job, list_jobs, remove_job


job_app = typer.Typer(help="Manage jobs.")


@job_app.command("add")
def job_add(name: str, command: str = typer.Option(..., "--command", "-c")) -> None:
    """Add a job."""
    conn = get_connection()
    try:
        add_job(conn, name, command)
        typer.echo(f"Added job '{name}'.")
    except JobAlreadyExistsError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1) from e
    finally:
        conn.close()


@job_app.command("list")
def job_list() -> None:
    """List all jobs."""
    conn = get_connection()
    try:
        jobs = list_jobs(conn)
        if not jobs:
            typer.echo("No jobs.")
        else:
            for job in jobs:
                typer.echo(f"{job.name}: {job.command}")
    finally:
        conn.close()


@job_app.command("remove")
def job_remove(name: str) -> None:
    """Remove a job."""
    conn = get_connection()
    try:
        remove_job(conn, name)
        typer.echo(f"Removed job '{name}'.")
    except JobNotFoundError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1) from e
    finally:
        conn.close()
