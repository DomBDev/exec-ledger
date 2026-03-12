from datetime import datetime

from execledger.models import Job, RunRecord


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
