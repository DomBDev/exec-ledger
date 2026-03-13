# ExecLedger

Local job runner for small automation tasks. Stores job definitions and run history in SQLite.

## Quick example

```bash
exl init
exl job add backup -c "cmd /c echo done"
exl run backup
exl history backup
```

## Development setup

```bash
git clone <repo-url>
cd exec-ledger
uv sync --dev
```

Or with pip:

```bash
git clone <repo-url>
cd exec-ledger
python -m venv .venv
.venv\Scripts\activate
pip install -e .
```

## CLI usage

```text
exl init                     Create .execledger/ and the database
exl job add <name> -c <cmd>  Add a job
exl job list                 List jobs
exl job remove <name>        Remove a job
exl run <name>               Run a job
exl history <name>           Show run history
```

On Windows, use `cmd /c` for shell built-ins such as `echo`. On Unix, commands run directly.

## Development

```bash
uv sync --dev
uv run pytest
uv run ruff check src tests
```

## Roadmap

* v0.0.1: CLI, foreground job execution, run history in SQLite
* later: background execution, log files, configurable paths
