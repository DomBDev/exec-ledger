from dataclasses import dataclass
from datetime import datetime


@dataclass
class Job:
    name: str
    command: str


@dataclass
class RunRecord:
    job_name: str
    started_at: datetime
    finished_at: datetime
    exit_code: int
    stdout: str
    stderr: str
