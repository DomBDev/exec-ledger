import shlex
import subprocess
import sys

from execledger.errors import ExecutionError


def run_job(command: str | list[str]) -> tuple[int, str, str]:
    """Execute a command and return (exit_code, stdout, stderr).

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
                    a[1:-1] if len(a) >= 2 and a[0] == a[-1] and a[0] in '"\'' else a
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
