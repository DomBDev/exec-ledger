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


@dataclass
class Pipeline:
    name: str
    created_at: datetime


@dataclass
class Step:
    pipeline_name: str
    name: str
    command: str | None
    func_ref: str | None
    position: int


@dataclass
class PipelineRun:
    id: int | None
    pipeline_name: str
    started_at: datetime | None
    finished_at: datetime | None
    status: str


@dataclass
class StepRun:
    id: int | None
    run_id: int
    step_name: str
    status: str
    started_at: datetime | None
    finished_at: datetime | None
    exit_code: int | None
    stdout: str
    stderr: str
