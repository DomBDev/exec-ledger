import sqlite3

import pytest

from execledger.db import init_db
from execledger.errors import JobAlreadyExistsError
from execledger.repository import add_job


def test_add_job() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    add_job(conn, "backup", "echo done")
    row = conn.execute("SELECT name, command FROM jobs WHERE name='backup'").fetchone()
    assert row == ("backup", "echo done")
    conn.close()


def test_add_job_duplicate_raises() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    add_job(conn, "backup", "echo done")
    with pytest.raises(JobAlreadyExistsError):
        add_job(conn, "backup", "echo other")
    conn.close()
