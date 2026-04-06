import sys

from typer.testing import CliRunner

from execledger.cli import app
from execledger.commands.init import init_cmd

runner = CliRunner()


def test_run_pipeline_cli_success(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    init_cmd()
    runner.invoke(app, ["add", "p"])
    py = sys.executable
    runner.invoke(
        app,
        ["step", "add", "p", "a", "-c", f'{py} -c "print(1)"'],
    )
    r = runner.invoke(app, ["run", "p"])
    assert r.exit_code == 0
    assert "completed" in r.stdout


def test_run_pipeline_cli_missing_fails(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    init_cmd()
    r = runner.invoke(app, ["run", "nope"])
    assert r.exit_code != 0


def test_run_pipeline_cli_step_fails_exits_nonzero(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    init_cmd()
    runner.invoke(app, ["add", "p"])
    py = sys.executable
    runner.invoke(
        app,
        ["step", "add", "p", "bad", "-c", f'{py} -c "import sys; sys.exit(3)"'],
    )
    r = runner.invoke(app, ["run", "p"])
    assert r.exit_code != 0
    assert "failed" in (r.stdout + r.stderr).lower()


def test_resume_cli_no_resumable_fails(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    init_cmd()
    runner.invoke(app, ["add", "p"])
    py = sys.executable
    runner.invoke(
        app,
        ["step", "add", "p", "a", "-c", f'{py} -c "print(1)"'],
    )
    runner.invoke(app, ["run", "p"])
    r = runner.invoke(app, ["resume", "p"])
    assert r.exit_code != 0


def test_resume_cli_after_flaky_step(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    init_cmd()
    counter = tmp_path / "n.txt"
    counter.write_text("0")
    script = tmp_path / "bump.py"
    script.write_text(
        "import sys\n"
        "from pathlib import Path\n"
        "p = Path(sys.argv[1])\n"
        "n = int(p.read_text())\n"
        "p.write_text(str(n + 1))\n"
        "raise SystemExit(1 if n == 0 else 0)\n"
    )
    py = sys.executable
    runner.invoke(app, ["add", "p1"])
    runner.invoke(
        app,
        ["step", "add", "p1", "ok", "-c", f'{py} -c "print(1)"'],
    )
    runner.invoke(
        app,
        [
            "step",
            "add",
            "p1",
            "flaky",
            "-c",
            f'"{py}" "{script}" "{counter}"',
        ],
    )
    runner.invoke(
        app,
        ["step", "add", "p1", "last", "-c", f'{py} -c "print(3)"'],
    )
    r_fail = runner.invoke(app, ["run", "p1"])
    assert r_fail.exit_code != 0

    r_ok = runner.invoke(app, ["resume", "p1"])
    assert r_ok.exit_code == 0
    assert "resumed and completed" in r_ok.stdout
