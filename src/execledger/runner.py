import importlib
import shlex
import subprocess
import sys
import traceback

from execledger.errors import ExecutionError


def run_command(command: str | list[str]) -> tuple[int, str, str]:
    """Execute a shell command and return (exit_code, stdout, stderr).

    Accept a shell-style string parsed with shlex, or a list of args.
    Raise ExecutionError if the process cannot be started.
    """
    try:
        if isinstance(command, list):
            args = command
        else:
            posix = sys.platform != "win32"
            cmd = command
            if not posix:
                # Normalize shell-escaped quotes (from PowerShell \" -> ")
                cmd = cmd.replace('\\"', '"').replace("\\'", "'")
            args = shlex.split(cmd, posix=posix)
            if not posix:
                args = [
                    a[1:-1] if len(a) >= 2 and a[0] == a[-1] and a[0] in "\"'" else a
                    for a in args
                ]
    except ValueError as e:
        raise ExecutionError(f"Invalid command format: {e}") from e

    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=3600,
        )
    except (subprocess.SubprocessError, FileNotFoundError, OSError) as e:
        raise ExecutionError(str(e)) from e

    return result.returncode, result.stdout, result.stderr


# alias for old code that still calls run_job
run_job = run_command


def run_function(func_ref: str) -> tuple[int, str, str]:
    """Import and call a Python function. Returns (exit_code, stdout, stderr).

    func_ref format: "module.path:function_name"
    Returns exit_code 0 on success, 1 if the function raises.
    """
    try:
        module_path, func_name = func_ref.split(":", 1)
    except ValueError:
        raise ExecutionError(
            f"invalid func_ref '{func_ref}', expected 'module:function'"
        ) from None

    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        raise ExecutionError(f"cannot import '{module_path}': {e}") from e

    func = getattr(module, func_name, None)
    if func is None:
        raise ExecutionError(f"'{func_name}' not found in '{module_path}'")

    try:
        func()
        return 0, "", ""
    except Exception:
        return 1, "", traceback.format_exc()
