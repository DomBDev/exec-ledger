import shlex
import subprocess

from execledger.errors import ExecutionError


def run_job(command: str | list[str]) -> tuple[int, str, str]:
    """Execute a command and return (exit_code, stdout, stderr).

    Accept a shell-style string parsed with shlex, or a list of args.
    Raise ExecutionError if the process cannot be started.
    """
    args = command if isinstance(command, list) else shlex.split(command)
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
