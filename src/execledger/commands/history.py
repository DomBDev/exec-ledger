import typer

from execledger.db import get_connection
from execledger.errors import JobNotFoundError
from execledger.repository import get_history, get_job


def history(name: str) -> None:
    """Show the run history for a job."""
    conn = get_connection()
    try:
        get_job(conn, name)
        runs = get_history(conn, name)
    except JobNotFoundError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1) from e
    finally:
        conn.close()

    if not runs:
        typer.echo("No runs.")
        return

    for run in runs:
        started_at = run.started_at.strftime("%Y-%m-%d %H:%M:%S")
        typer.echo(f"{started_at}  exit {run.exit_code}")
