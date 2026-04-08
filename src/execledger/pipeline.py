from collections.abc import Callable
from datetime import datetime, timezone

from execledger.db import get_connection
from execledger.engine import resume_pipeline, restart_pipeline, run_pipeline
from execledger.errors import (
    PipelineNotFoundError,
    StepConfigurationError,
)
from execledger.models import PipelineRun, StepRun
from execledger.repository import (
    add_pipeline,
    add_step as repo_add_step,
    get_pipeline,
    get_pipeline_run_status,
    get_run_history,
    list_steps,
)


def _func_ref(func: Callable[..., object]) -> str:
    """Build module:func for run_function; module level callables only."""
    qual = getattr(func, "__qualname__", "")
    if not qual or "<locals>" in qual or "." in qual:
        raise StepConfigurationError(
            "func must be a module-level named function (not a method or nested def)"
        )
    mod = getattr(func, "__module__", None)
    if mod in (None, "builtins"):
        raise StepConfigurationError("func must be a plain module function")
    return f"{mod}:{qual}"


class Pipeline:
    """Load or create a named pipeline in the project local database."""

    def __init__(self, name: str) -> None:
        self.name = name
        conn = get_connection()
        try:
            try:
                get_pipeline(conn, name)
            except PipelineNotFoundError:
                add_pipeline(conn, name, datetime.now(timezone.utc))
        finally:
            conn.close()

    def add_step(
        self,
        name: str,
        command: str | None = None,
        func: Callable[..., object] | None = None,
    ) -> None:
        if command is not None and str(command).strip() and func is not None:
            raise StepConfigurationError("step cannot set both command and func")
        if func is None and (command is None or not str(command).strip()):
            raise StepConfigurationError("step needs command or func")

        cmd: str | None = None
        func_ref: str | None = None
        if func is not None:
            func_ref = _func_ref(func)
        else:
            cmd = command

        conn = get_connection()
        try:
            steps = list_steps(conn, self.name)
            pos = len(steps)
            repo_add_step(conn, self.name, name, pos, command=cmd, func_ref=func_ref)
        finally:
            conn.close()

    def run(self) -> int:
        conn = get_connection()
        try:
            return run_pipeline(conn, self.name)
        finally:
            conn.close()

    def resume(self) -> int:
        conn = get_connection()
        try:
            return resume_pipeline(conn, self.name)
        finally:
            conn.close()

    def restart(self) -> int:
        conn = get_connection()
        try:
            return restart_pipeline(conn, self.name)
        finally:
            conn.close()

    def status(self) -> tuple[PipelineRun | None, list[StepRun]]:
        """Latest run summary and its step rows, or (None, []) if no runs."""
        conn = get_connection()
        try:
            hist = get_run_history(conn, self.name)
            if not hist:
                return (None, [])
            latest = hist[0]
            rid = latest.id
            if rid is None:
                return (latest, [])
            run, step_runs = get_pipeline_run_status(conn, rid)
            return (run, step_runs)
        finally:
            conn.close()
