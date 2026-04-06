import sqlite3

from execledger.commands.init import init_cmd
from execledger.db import get_db_path


def test_init_creates_dir_and_db(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    init_cmd()

    db_path = get_db_path()
    assert db_path.parent.exists()
    assert db_path.exists()

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("SELECT 1 FROM pipelines LIMIT 1")
        conn.execute("SELECT 1 FROM pipeline_runs LIMIT 1")


def test_init_idempotent(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    init_cmd()
    init_cmd()

    db_path = get_db_path()
    assert db_path.exists()

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("SELECT 1 FROM pipelines LIMIT 1")
