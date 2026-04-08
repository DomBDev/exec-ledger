from typer.testing import CliRunner

from execledger.cli import app
from execledger.commands.init import init_cmd

runner = CliRunner()


def test_pipeline_add_list_remove(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    init_cmd()

    r = runner.invoke(app, ["add", "demo"])
    assert r.exit_code == 0
    assert "Added pipeline 'demo'" in r.stdout

    r = runner.invoke(app, ["list"])
    assert r.exit_code == 0
    assert "demo" in r.stdout

    r = runner.invoke(app, ["remove", "demo"])
    assert r.exit_code == 0

    r = runner.invoke(app, ["list"])
    assert r.exit_code == 0
    assert "No pipelines" in r.stdout


def test_pipeline_add_duplicate_fails(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    init_cmd()
    assert runner.invoke(app, ["add", "x"]).exit_code == 0
    dup = runner.invoke(app, ["add", "x"])
    assert dup.exit_code != 0


def test_pipeline_status_no_runs(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    init_cmd()
    runner.invoke(app, ["add", "p"])
    r = runner.invoke(app, ["status", "p"])
    assert r.exit_code == 0
    assert "no runs yet" in r.stdout


def test_pipeline_remove_missing_fails(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    init_cmd()
    r = runner.invoke(app, ["remove", "nope"])
    assert r.exit_code != 0


def test_pipeline_status_missing_fails(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    init_cmd()
    r = runner.invoke(app, ["status", "nope"])
    assert r.exit_code != 0
