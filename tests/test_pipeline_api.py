import sys

import pytest

from execledger import Pipeline
from execledger.db import get_connection
from execledger.errors import (
    NoResumableRunError,
    StepAlreadyExistsError,
    StepConfigurationError,
)
from execledger.repository import get_run_history, list_steps


def api_noop() -> None:
    pass


def test_pipeline_exported_from_package() -> None:
    assert Pipeline is not None


def test_pipeline_creates_and_loads(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    p = Pipeline("my-pipe")
    p2 = Pipeline("my-pipe")
    assert p.name == p2.name


def test_add_step_run_status_command(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    py = sys.executable
    p = Pipeline("p1")
    p.add_step("a", command=f'{py} -c "print(1)"')
    rid = p.run()
    run, steps = p.status()
    assert run is not None
    assert run.id == rid
    assert run.status == "completed"
    assert len(steps) == 1
    assert steps[0].status == "completed"


def test_add_step_func_module_level(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    p = Pipeline("p1")
    p.add_step("fn", func=api_noop)
    rid = p.run()
    run, steps = p.status()
    assert run is not None and run.id == rid
    assert run.status == "completed"
    assert steps[0].status == "completed"


def test_add_step_both_command_and_func_raises(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    p = Pipeline("p1")
    with pytest.raises(StepConfigurationError, match="both"):
        p.add_step("x", command="echo hi", func=api_noop)


def test_add_step_neither_raises(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    p = Pipeline("p1")
    with pytest.raises(StepConfigurationError, match="needs command or func"):
        p.add_step("x")


def test_add_step_duplicate_name_raises(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    py = sys.executable
    p = Pipeline("p1")
    p.add_step("a", command=f'{py} -c "print(1)"')
    with pytest.raises(StepAlreadyExistsError):
        p.add_step("a", command=f'{py} -c "print(2)"')


def test_resume_after_failure(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    counter = tmp_path / "n.txt"
    counter.write_text("0")
    script = tmp_path / "bump.py"
    script.write_text(
        "import sys\n"
        "from pathlib import Path\n"
        "p = Path(sys.argv[1])\n"
        "n = int(p.read_text())\n"
        "p.write_text(str(n + 1))\n"
        "raise SystemExit(1 if n == 0 else 0)\n"
    )
    py = sys.executable
    p = Pipeline("p1")
    p.add_step("ok", command=f'{py} -c "print(1)"')
    p.add_step("flaky", command=f'"{py}" "{script}" "{counter}"')
    p.add_step("last", command=f'{py} -c "print(3)"')

    run_id = p.run()
    run, _ = p.status()
    assert run is not None and run.id == run_id
    assert run.status == "failed"

    assert p.resume() == run_id
    run2, steps = p.status()
    assert run2 is not None and run2.status == "completed"
    assert len(steps) == 3
    assert steps[2].step_name == "last"


def test_restart_is_new_run(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    py = sys.executable
    p = Pipeline("p1")
    p.add_step("a", command=f'{py} -c "print(1)"')
    p.add_step("b", command=f'{py} -c "import sys; sys.exit(1)"')
    first = p.run()
    second = p.restart()
    assert second != first
    conn = get_connection()
    try:
        hist = get_run_history(conn, "p1")
        assert len(hist) == 2
    finally:
        conn.close()


def test_resume_raises_when_nothing_to_resume(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    py = sys.executable
    p = Pipeline("p1")
    p.add_step("a", command=f'{py} -c "print(1)"')
    p.run()
    with pytest.raises(NoResumableRunError):
        p.resume()


def test_second_instance_sees_steps_from_first(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    py = sys.executable
    Pipeline("shared").add_step("only", command=f'{py} -c "print(1)"')
    conn = get_connection()
    try:
        assert len(list_steps(conn, "shared")) == 1
    finally:
        conn.close()
    p = Pipeline("shared")
    assert p.run() > 0


def test_lambda_func_rejected(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    p = Pipeline("p1")
    with pytest.raises(StepConfigurationError, match="module-level"):
        p.add_step("x", func=lambda: None)
