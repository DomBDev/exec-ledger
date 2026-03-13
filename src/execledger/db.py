from pathlib import Path


def get_db_path() -> Path:
    """Path to .execledger/execledger.db (project-local)."""
    return Path(".execledger") / "execledger.db"
