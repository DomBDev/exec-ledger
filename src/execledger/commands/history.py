import typer

from execledger.db import get_connection
from execledger.errors import PipelineNotFoundError
from execledger.models import PipelineRun
from execledger.repository import (
    get_all_pipeline_run_history,
    get_pipeline,
    get_run_history,
)


def _fmt_run_line(run: PipelineRun, *, show_pipeline: bool) -> str:
    started = run.started_at.strftime("%Y-%m-%d %H:%M:%S") if run.started_at else "-"
    rid = run.id if run.id is not None else "-"
    if show_pipeline:
        return f"{started}  {run.pipeline_name}  {run.status}  run {rid}"
    return f"{started}  {run.status}  run {rid}"


def history(
    name: str | None = typer.Argument(
        None, help="Pipeline name. Omit to show runs for all pipelines."
    ),
) -> None:
    """Show pipeline run history."""
    conn = get_connection()
    try:
        if name is not None:
            get_pipeline(conn, name)
            runs = get_run_history(conn, name)
        else:
            runs = get_all_pipeline_run_history(conn)
    except PipelineNotFoundError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1) from e
    finally:
        conn.close()

    if not runs:
        typer.echo("No runs.")
        return

    show_pipeline = name is None
    for run in runs:
        typer.echo(_fmt_run_line(run, show_pipeline=show_pipeline))
