class JobNotFoundError(Exception):
    """Job name does not exist."""


class JobAlreadyExistsError(Exception):
    """Duplicate job name on add."""


class ExecutionError(Exception):
    """Run failed (subprocess or command parse error)."""


class SchedulerError(Exception):
    """Schedule related failures."""


class PipelineNotFoundError(Exception):
    """Pipeline name does not exist."""


class PipelineAlreadyExistsError(Exception):
    """Duplicate pipeline name on add."""


class StepConfigurationError(Exception):
    """Step must have exactly one of command or func_ref."""
