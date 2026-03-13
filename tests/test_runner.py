import sys

import pytest

from execledger.errors import ExecutionError
from execledger.runner import run_job


def test_run_job_captures_stdout() -> None:
    exit_code, stdout, stderr = run_job(
        [sys.executable, "-c", "print('done')"]
    )
    assert exit_code == 0
    assert stdout.strip() == "done"
    assert stderr == ""


def test_run_job_captures_exit_code() -> None:
    exit_code, stdout, stderr = run_job(
        [sys.executable, "-c", "import sys; sys.exit(1)"]
    )
    assert exit_code == 1
    assert stdout == ""
    assert stderr == ""


def test_run_job_captures_stderr() -> None:
    exit_code, stdout, stderr = run_job(
        [sys.executable, "-c", "import sys; print('err', file=sys.stderr)"]
    )
    assert exit_code == 0
    assert stdout == ""
    assert "err" in stderr


def test_run_job_accepts_string_command() -> None:
    exit_code, stdout, stderr = run_job('python -c "print(\'done\')"')
    assert exit_code == 0
    assert stdout.strip() == "done"
    assert stderr == ""


def test_run_job_command_not_found_raises() -> None:
    with pytest.raises(ExecutionError):
        run_job("nonexistent_command_xyz")
