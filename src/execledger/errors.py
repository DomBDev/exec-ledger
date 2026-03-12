class JobNotFoundError(Exception):
    """Job name does not exist."""


class JobAlreadyExistsError(Exception):
    """Duplicate job name on add."""


class ExecutionError(Exception):
    """Run failed (subprocess error, nonzero exit)."""


class SchedulerError(Exception):
    """Schedule related failures."""
