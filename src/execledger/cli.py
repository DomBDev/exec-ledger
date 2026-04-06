import typer

from execledger.commands.history import history
from execledger.commands.init import init_cmd
from execledger.commands.pipeline import (
    pipeline_add,
    pipeline_list,
    pipeline_remove,
    pipeline_status,
)
from execledger.commands.run import run
from execledger.commands.step import step_app

app = typer.Typer()


@app.callback()
def main() -> None:
    """ExecLedger, local pipeline runner."""
    pass


app.command("init")(init_cmd)
app.command("add")(pipeline_add)
app.command("list")(pipeline_list)
app.command("remove")(pipeline_remove)
app.command("status")(pipeline_status)
app.add_typer(step_app, name="step")
app.command("run")(run)
app.command("history")(history)
