import typer

job_app = typer.Typer(help="Manage jobs.")


@job_app.command("add")
def job_add(name: str, command: str = typer.Option(..., "--command", "-c")) -> None:
    """Add a job."""
    typer.echo("not implemented")


@job_app.command("list")
def job_list() -> None:
    """List all jobs."""
    typer.echo("not implemented")


@job_app.command("remove")
def job_remove(name: str) -> None:
    """Remove a job."""
    typer.echo("not implemented")
