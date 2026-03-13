import sqlite3

import typer

from execledger.db import get_db_path, init_db


def init_cmd() -> None:
    """Create .execledger/ and initialize the database."""
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(str(db_path)) as conn:
        init_db(conn)

    typer.echo("Initialized.")
