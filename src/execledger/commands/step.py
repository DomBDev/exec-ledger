import typer

from execledger.db import get_connection
from execledger.errors import (
    PipelineNotFoundError,
    StepAlreadyExistsError,
    StepConfigurationError,
    StepNotFoundError,
)
from execledger.repository import add_step, get_pipeline, list_steps, remove_step

step_app = typer.Typer(help="Manage steps on a pipeline.")


def _normalize_step_inputs(
    command: str | None, func: str | None
) -> tuple[str | None, str | None]:
    """Exactly one of command or func string; wording matches engine._step_kind."""
    cmd = (command or "").strip()
    fn = (func or "").strip()
    has_cmd = bool(cmd)
    has_fn = bool(fn)
    if has_cmd and has_fn:
        raise StepConfigurationError("step cannot set both command and func_ref")
    if not has_cmd and not has_fn:
        raise StepConfigurationError("step needs command or func_ref")
    if has_cmd:
        return cmd, None
    return None, fn


@step_app.command("add")
def step_add(
    pipeline: str,
    name: str,
    command: str | None = typer.Option(None, "--command", "-c"),
    func: str | None = typer.Option(None, "--func"),
) -> None:
    """Append a step; order is the order steps were added."""
    try:
        cmd, fn = _normalize_step_inputs(command, func)
    except StepConfigurationError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1) from e

    conn = get_connection()
    try:
        pos = len(list_steps(conn, pipeline))
        add_step(conn, pipeline, name, pos, command=cmd, func_ref=fn)
        typer.echo(f"Added step '{name}' to pipeline '{pipeline}'.")
    except PipelineNotFoundError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1) from e
    except StepAlreadyExistsError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1) from e
    finally:
        conn.close()


@step_app.command("list")
def step_list(pipeline: str) -> None:
    """List steps for a pipeline in run order."""
    conn = get_connection()
    try:
        get_pipeline(conn, pipeline)
        steps = list_steps(conn, pipeline)
        if not steps:
            typer.echo("No steps.")
        else:
            for s in steps:
                if s.command:
                    typer.echo(f"{s.name}: {s.command}")
                elif s.func_ref:
                    typer.echo(f"{s.name}: func {s.func_ref}")
                else:
                    typer.echo(f"{s.name}:")
    except PipelineNotFoundError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1) from e
    finally:
        conn.close()


@step_app.command("remove")
def step_remove(pipeline: str, name: str) -> None:
    """Remove a step from a pipeline."""
    conn = get_connection()
    try:
        remove_step(conn, pipeline, name)
        typer.echo(f"Removed step '{name}' from pipeline '{pipeline}'.")
    except (PipelineNotFoundError, StepNotFoundError) as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1) from e
    finally:
        conn.close()
