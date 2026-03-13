import typer

from execledger.commands.history import history
from execledger.commands.job import job_app
from execledger.commands.run import run

app = typer.Typer()


@app.callback()
def main() -> None:
    """ExecLedger, local job runner."""
    pass


app.add_typer(job_app, name="job")
app.command("run")(run)
app.command("history")(history)
