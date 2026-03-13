import sys

import pytest

from execledger.errors import ExecutionError
from execledger.runner import run_job


def test_run_job_captures_stdout() -> None:
    exit_code, stdout, stderr = run_job([sys.executable, "-c", "print('done')"])
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
    cmd = f'{sys.executable} -c "print(1)"'
    exit_code, stdout, stderr = run_job(cmd)
    assert exit_code == 0
    assert stdout.strip() == "1"
    assert stderr == ""


def test_run_job_command_not_found_raises() -> None:
    with pytest.raises(ExecutionError):
        run_job("nonexistent_command_xyz")


def test_run_job_malformed_string_raises() -> None:
    """Unclosed quote raises ExecutionError (shlex fails before execution)."""
    with pytest.raises(ExecutionError, match="Invalid command format"):
        run_job('echo "unclosed')


def test_run_job_quoted_arg_with_space() -> None:
    """Quoted arg with space parses as single token."""
    exit_code, stdout, _ = run_job(
        f'{sys.executable} -c "import sys; print(sys.argv[1])" "hello world"'
    )
    assert exit_code == 0
    assert stdout.strip() == "hello world"


def test_run_job_list_and_string_same_result() -> None:
    """List input and equivalent string produce same result."""
    list_result = run_job([sys.executable, "-c", "print(42)"])
    str_result = run_job(f'{sys.executable} -c "print(42)"')
    assert list_result == str_result
