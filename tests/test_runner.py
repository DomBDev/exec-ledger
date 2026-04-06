import sys

import pytest

from execledger.errors import ExecutionError
from execledger.runner import run_command, run_function


def test_run_command_captures_stdout() -> None:
    exit_code, stdout, stderr = run_command([sys.executable, "-c", "print('done')"])
    assert exit_code == 0
    assert stdout.strip() == "done"
    assert stderr == ""


def test_run_command_captures_exit_code() -> None:
    exit_code, stdout, stderr = run_command(
        [sys.executable, "-c", "import sys; sys.exit(1)"]
    )
    assert exit_code == 1
    assert stdout == ""
    assert stderr == ""


def test_run_command_captures_stderr() -> None:
    exit_code, stdout, stderr = run_command(
        [sys.executable, "-c", "import sys; print('err', file=sys.stderr)"]
    )
    assert exit_code == 0
    assert stdout == ""
    assert "err" in stderr


def test_run_command_accepts_string() -> None:
    cmd = f'{sys.executable} -c "print(1)"'
    exit_code, stdout, stderr = run_command(cmd)
    assert exit_code == 0
    assert stdout.strip() == "1"
    assert stderr == ""


def test_run_command_not_found_raises() -> None:
    with pytest.raises(ExecutionError):
        run_command("nonexistent_command_xyz")


def test_run_command_malformed_string_raises() -> None:
    with pytest.raises(ExecutionError, match="Invalid command format"):
        run_command('echo "unclosed')


def test_run_command_quoted_arg_with_space() -> None:
    exit_code, stdout, _ = run_command(
        f'{sys.executable} -c "import sys; print(sys.argv[1])" "hello world"'
    )
    assert exit_code == 0
    assert stdout.strip() == "hello world"


def test_run_command_list_and_string_same_result() -> None:
    list_result = run_command([sys.executable, "-c", "print(42)"])
    str_result = run_command(f'{sys.executable} -c "print(42)"')
    assert list_result == str_result


def test_run_function_success() -> None:
    exit_code, _, stderr = run_function("os:getcwd")
    assert exit_code == 0
    assert stderr == ""


def test_run_function_exception_returns_exit_1() -> None:
    """Calling a function that raises gives exit_code=1 and traceback in stderr."""
    exit_code, _, stderr = run_function("json:loads")
    assert exit_code == 1
    assert "TypeError" in stderr


def test_run_function_bad_format_raises() -> None:
    with pytest.raises(ExecutionError, match="invalid func_ref"):
        run_function("no_colon_here")


def test_run_function_bad_module_raises() -> None:
    with pytest.raises(ExecutionError, match="cannot import"):
        run_function("nonexistent_module_xyz:func")


def test_run_function_bad_func_raises() -> None:
    with pytest.raises(ExecutionError, match="not found in"):
        run_function("os.path:nonexistent_func_xyz")
