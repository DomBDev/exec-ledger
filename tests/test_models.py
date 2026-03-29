from datetime import datetime, timezone

from execledger.models import Job, Pipeline, PipelineRun, RunRecord, Step, StepRun


def test_job_creation() -> None:
    job = Job(name="backup", command="echo done")
    assert job.name == "backup"
    assert job.command == "echo done"


def test_run_record_creation() -> None:
    started = datetime.now()
    record = RunRecord(
        job_name="backup",
        started_at=started,
        finished_at=started,
        exit_code=0,
        stdout="done",
        stderr="",
    )
    assert record.job_name == "backup"
    assert record.exit_code == 0
    assert record.stdout == "done"


def test_pipeline_creation() -> None:
    now = datetime.now(timezone.utc)
    p = Pipeline(name="deploy", created_at=now)
    assert p.name == "deploy"
    assert p.created_at == now


def test_step_creation() -> None:
    step = Step(
        pipeline_name="deploy",
        name="build",
        command="make build",
        func_ref=None,
        position=0,
    )
    assert step.pipeline_name == "deploy"
    assert step.name == "build"
    assert step.command == "make build"
    assert step.func_ref is None
    assert step.position == 0


def test_step_with_func_ref() -> None:
    step = Step(
        pipeline_name="deploy",
        name="transform",
        command=None,
        func_ref="mymodule:transform_data",
        position=1,
    )
    assert step.command is None
    assert step.func_ref == "mymodule:transform_data"


def test_pipeline_run_creation() -> None:
    now = datetime.now(timezone.utc)
    run = PipelineRun(
        id=None,
        pipeline_name="deploy",
        started_at=now,
        finished_at=None,
        status="running",
    )
    assert run.id is None
    assert run.pipeline_name == "deploy"
    assert run.status == "running"
    assert run.finished_at is None


def test_step_run_creation() -> None:
    sr = StepRun(
        id=None,
        run_id=1,
        step_name="build",
        status="pending",
        started_at=None,
        finished_at=None,
        exit_code=None,
        stdout="",
        stderr="",
    )
    assert sr.run_id == 1
    assert sr.step_name == "build"
    assert sr.status == "pending"
    assert sr.exit_code is None
