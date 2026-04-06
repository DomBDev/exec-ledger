import sys
from datetime import datetime, timezone

import pytest
from typer import Exit

from execledger.commands.history import history
from execledger.commands.init import init_cmd
from execledger.db import get_connection
from execledger.engine import run_pipeline
from execledger.repository import add_pipeline, add_step


def test_history_shows_runs(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    init_cmd()
    py = sys.executable
    conn = get_connection()
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "p", now)
    add_step(conn, "p", "a", 0, command=f'{py} -c "print(1)"')
    run_pipeline(conn, "p")
    run_pipeline(conn, "p")
    conn.close()

    history("p")

    out, err = capsys.readouterr()
    assert err == ""
    assert out.count("completed") == 2
    assert "run" in out


def test_history_no_runs(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    init_cmd()
    conn = get_connection()
    add_pipeline(conn, "empty", datetime.now(timezone.utc))
    conn.close()

    history("empty")

    out, err = capsys.readouterr()
    assert err == ""
    assert "No runs." in out


def test_history_pipeline_not_found(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    init_cmd()

    with pytest.raises(Exit) as exc_info:
        history("nonexistent")

    out, err = capsys.readouterr()
    assert exc_info.value.exit_code == 1
    assert "not found" in err.lower()


def test_history_all_runs(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    init_cmd()
    py = sys.executable
    conn = get_connection()
    now = datetime.now(timezone.utc)
    add_pipeline(conn, "p1", now)
    add_pipeline(conn, "p2", now)
    add_step(conn, "p1", "a", 0, command=f'{py} -c "print(1)"')
    add_step(conn, "p2", "b", 0, command=f'{py} -c "print(2)"')
    run_pipeline(conn, "p1")
    run_pipeline(conn, "p2")
    conn.close()

    history(None)

    out, err = capsys.readouterr()
    assert err == ""
    assert "p1" in out
    assert "p2" in out
    assert "completed" in out
