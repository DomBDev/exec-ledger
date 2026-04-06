# ExecLedger

Local pipeline runner for small automation. Ordered steps run one after another. Status and history live in SQLite under `.execledger/` in whatever directory you use the tool from.

Library: import `Pipeline` in Python or drive the same data with the `exl` CLI. Synchronous runs, optional **resume** after a failure instead of starting from step one.

## Quick example

```bash
exl init
exl add backup
exl step add backup fetch -c "python -c \"print(1)\""
exl step add backup save -c "python -c \"print(2)\""
exl run backup
exl history backup
```

Same idea from code (same DB rules as the CLI):

```python
from execledger import Pipeline

p = Pipeline("backup")
p.add_step("fetch", command="python -c \"print(1)\"")
p.add_step("save", command="python -c \"print(2)\"")
p.run()
```

## Installation

With `uv`, from another project:

```bash
uv add "exec-ledger @ git+https://github.com/DomBDev/exec-ledger.git"
```

## Development setup

```bash
git clone https://github.com/DomBDev/exec-ledger.git
cd exec-ledger
uv sync --dev
```

Or with pip:

```bash
git clone https://github.com/DomBDev/exec-ledger.git
cd exec-ledger
python -m venv .venv
.venv\Scripts\activate
pip install -e .
```

## CLI usage

```text
exl init                          Create .execledger/ and the database
exl add <name>                    Add an empty pipeline
exl list                          List pipelines
exl remove <name>                Remove a pipeline and its steps
exl status <name>                Latest run and per step rows
exl step add <pipeline> <step> -c "<cmd>"
exl step add <pipeline> <step> --func "module:func"
exl step list <pipeline>
exl step remove <pipeline> <step>
exl run <name>                    Run all steps once
exl resume <name>                Continue the latest failed/interrupted run
exl history [name]                Run history. Omit name for all pipelines
```

Exactly one of `-c` or `--func` per step. Step order is the order you add them.

On Windows, shell builtins usually need `cmd /c ...` because commands run without `shell=True`.

## Development

```bash
uv sync --dev
uv run pytest
uv run ruff check src tests
```

## Roadmap

**v0.0.3** (current focus, alpha development): pipeline + steps, status, resume/restart, SQLite history, legacy “job” model removed for new databases.

**v0.1.0 onward:** passing data between steps, run parameters, richer step contracts (not in the current release).

**Further out:** retries and timeouts, background runs.
