import typer

from execledger.db import get_connection
from execledger.engine import resume_pipeline, run_pipeline
from execledger.errors import (
    NoResumableRunError,
    PipelineNotFoundError,
    StepConfigurationError,
)
from execledger.repository import get_pipeline_run_status


def run(name: str) -> None:
    """Run all steps of a pipeline once."""
    conn = get_connection()
    try:
        run_id = run_pipeline(conn, name)
        run_row, _ = get_pipeline_run_status(conn, run_id)
    except PipelineNotFoundError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1) from e
    except StepConfigurationError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1) from e
    finally:
        conn.close()

    if run_row.status == "completed":
        typer.echo(f"Run {run_id} completed.")
        raise typer.Exit(0)
    typer.echo(f"Run {run_id} failed.", err=True)
    raise typer.Exit(1)


def resume(name: str) -> None:
    """Resume the latest failed or interrupted run for this pipeline."""
    conn = get_connection()
    try:
        run_id = resume_pipeline(conn, name)
        run_row, _ = get_pipeline_run_status(conn, run_id)
    except PipelineNotFoundError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1) from e
    except NoResumableRunError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1) from e
    except StepConfigurationError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1) from e
    finally:
        conn.close()

    if run_row.status == "completed":
        typer.echo(f"Run {run_id} resumed and completed.")
        raise typer.Exit(0)
    typer.echo(f"Run {run_id} failed.", err=True)
    raise typer.Exit(1)
