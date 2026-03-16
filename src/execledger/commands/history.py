import typer

from execledger.db import get_connection
from execledger.errors import JobNotFoundError
from execledger.repository import get_all_history, get_history, get_job


def history(
    name: str | None = typer.Argument(None, help="Job name. Omit to show all runs."),
) -> None:
    """Show run history. With job name: runs for that job. Without: all runs."""
    conn = get_connection()
    try:
        if name is not None:
            get_job(conn, name)
            runs = get_history(conn, name)
        else:
            runs = get_all_history(conn)
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
        if name is not None:
            typer.echo(f"{started_at}  exit {run.exit_code}")
        else:
            typer.echo(f"{started_at}  {run.job_name}  exit {run.exit_code}")
