import typer

from execledger.commands.history import history
from execledger.commands.init import init_cmd
from execledger.commands.job import job_app
from execledger.commands.run import run

app = typer.Typer()


@app.callback()
def main() -> None:
    """ExecLedger, local job runner."""
    pass


app.command("init")(init_cmd)
app.add_typer(job_app, name="job")
app.command("run")(run)
app.command("history")(history)
