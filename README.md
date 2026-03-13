# ExecLedger

ExecLedger is a versatile job runner I’m building for small automation tasks.

Right now this project is still early. The SQLite storage layer and repository functions are in place, but the CLI and actual job execution are still being built.

## Current state

So far, the project has:

- SQLite database setup
- job and run models
- custom domain errors
- repository functions for adding, listing, removing, and looking up jobs
- run history storage

## Planned next

- wire the CLI to the repository layer
- execute jobs and capture real run output
- improve error handling and command flow

## Development

```bash
uv sync
uv run pytest
```