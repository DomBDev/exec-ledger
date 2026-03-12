import pytest

from execledger.errors import JobNotFoundError


def test_job_not_found_raises() -> None:
    with pytest.raises(JobNotFoundError):
        raise JobNotFoundError("no such job")