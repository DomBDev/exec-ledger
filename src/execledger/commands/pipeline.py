from datetime import datetime, timezone

import typer

from execledger.db import get_connection
from execledger.errors import PipelineAlreadyExistsError, PipelineNotFoundError
from execledger.repository import (
    add_pipeline,
    get_pipeline,
    get_pipeline_run_status,
    get_run_history,
    list_pipelines,
    remove_pipeline,
)


def pipeline_add(name: str) -> None:
    """Add an empty pipeline (steps are added with exl step add)."""
    conn = get_connection()
    try:
        add_pipeline(conn, name, datetime.now(timezone.utc))
        typer.echo(f"Added pipeline '{name}'.")
    except PipelineAlreadyExistsError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1) from e
    finally:
        conn.close()


def pipeline_list() -> None:
    """List all pipelines."""
    conn = get_connection()
    try:
        rows = list_pipelines(conn)
        if not rows:
            typer.echo("No pipelines.")
        else:
            for p in rows:
                typer.echo(p.name)
    finally:
        conn.close()


def pipeline_remove(name: str) -> None:
    """Remove a pipeline and its steps."""
    conn = get_connection()
    try:
        remove_pipeline(conn, name)
        typer.echo(f"Removed pipeline '{name}'.")
    except PipelineNotFoundError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1) from e
    finally:
        conn.close()


def pipeline_status(
    name: str = typer.Argument(
        ...,
        help="Pipeline name. Run ids shown are global (pipeline_runs.id).",
    ),
) -> None:
    """Show the latest run and step rows for a pipeline."""
    conn = get_connection()
    try:
        get_pipeline(conn, name)
        hist = get_run_history(conn, name)
        if not hist:
            typer.echo(f"Pipeline '{name}' has no runs yet.")
            return
        latest = hist[0]
        rid = latest.id
        if rid is None:
            typer.echo(f"latest: status={latest.status!r} (missing run id)")
            return
        run, step_runs = get_pipeline_run_status(conn, rid)
        typer.echo(f"latest global run id={run.id} status={run.status!r}")
        for s in step_runs:
            typer.echo(
                f"  {s.step_name}: {s.status} exit={s.exit_code} "
                f"started={s.started_at} finished={s.finished_at}"
            )
    except PipelineNotFoundError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1) from e
    finally:
        conn.close()
