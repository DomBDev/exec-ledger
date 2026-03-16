from datetime import datetime, timezone

import pytest
from typer import Exit

from execledger.commands.history import history
from execledger.commands.init import init_cmd
from execledger.db import get_connection
from execledger.models import RunRecord
from execledger.repository import add_job, add_run


def test_history_shows_runs(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    init_cmd()

    conn = get_connection()
    add_job(conn, "testjob", "echo ok")

    first = datetime(2026, 3, 13, 12, 0, 0, tzinfo=timezone.utc)
    second = datetime(2026, 3, 13, 12, 5, 0, tzinfo=timezone.utc)

    add_run(conn, RunRecord("testjob", first, first, 0, "out", ""))
    add_run(conn, RunRecord("testjob", second, second, 1, "", "err"))
    conn.close()

    history("testjob")

    out, err = capsys.readouterr()

    assert err == ""
    assert "2026-03-13 12:00:00  exit 0" in out
    assert "2026-03-13 12:05:00  exit 1" in out


def test_history_no_runs(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    init_cmd()

    conn = get_connection()
    add_job(conn, "empty", "echo x")
    conn.close()

    history("empty")

    out, err = capsys.readouterr()

    assert err == ""
    assert "No runs." in out


def test_history_job_not_found(tmp_path, monkeypatch, capsys) -> None:
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

    conn = get_connection()
    add_job(conn, "job1", "echo 1")
    add_job(conn, "job2", "echo 2")
    now = datetime(2026, 3, 13, 12, 0, 0, tzinfo=timezone.utc)
    add_run(conn, RunRecord("job1", now, now, 0, "", ""))
    add_run(conn, RunRecord("job2", now, now, 1, "", ""))
    conn.close()

    history(None)

    out, err = capsys.readouterr()

    assert err == ""
    assert "job1" in out
    assert "job2" in out
    assert "exit 0" in out
    assert "exit 1" in out
