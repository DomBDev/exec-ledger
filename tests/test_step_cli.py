import sys

from typer.testing import CliRunner

from execledger.cli import app
from execledger.commands.init import init_cmd

runner = CliRunner()


def test_step_add_list_remove_command(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    init_cmd()
    runner.invoke(app, ["add", "demo"])
    py = sys.executable.replace("\\", "/")
    r = runner.invoke(
        app,
        ["step", "add", "demo", "a", "-c", f'{py} -c "print(1)"'],
    )
    assert r.exit_code == 0
    r = runner.invoke(app, ["step", "list", "demo"])
    assert r.exit_code == 0
    assert "a:" in r.stdout
    r = runner.invoke(app, ["step", "remove", "demo", "a"])
    assert r.exit_code == 0
    r = runner.invoke(app, ["step", "list", "demo"])
    assert r.exit_code == 0
    assert "No steps" in r.stdout


def test_step_add_func_and_order(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    init_cmd()
    runner.invoke(app, ["add", "p"])
    r = runner.invoke(app, ["step", "add", "p", "fn", "--func", "os:getcwd"])
    assert r.exit_code == 0
    r = runner.invoke(app, ["step", "list", "p"])
    assert "func os:getcwd" in r.stdout


def test_step_list_pipeline_missing_fails(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    init_cmd()
    r = runner.invoke(app, ["step", "list", "nope"])
    assert r.exit_code != 0


def test_step_add_duplicate_fails(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    init_cmd()
    runner.invoke(app, ["add", "p"])
    py = sys.executable
    invoke = ["step", "add", "p", "x", "-c", f'{py} -c "print(1)"']
    assert runner.invoke(app, invoke).exit_code == 0
    r = runner.invoke(app, invoke)
    assert r.exit_code != 0


def test_step_add_both_command_and_func_fails(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    init_cmd()
    runner.invoke(app, ["add", "p"])
    r = runner.invoke(
        app,
        ["step", "add", "p", "x", "-c", "echo hi", "--func", "os:getcwd"],
    )
    assert r.exit_code != 0


def test_step_add_neither_command_nor_func_fails(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    init_cmd()
    runner.invoke(app, ["add", "p"])
    r = runner.invoke(app, ["step", "add", "p", "x"])
    assert r.exit_code != 0
