"""Test for local job executor."""

import os

import pytest

from prefect_miyabi.core import MiyabiJobBlock


async def test_local(tmp_path, caplog):
    """Test executing `echo` command with the local executor."""
    block = MiyabiJobBlock(
        work_root=str(tmp_path),
        executable="echo",
        executor="local",
        launcher="single",
    )

    with block.get_executor() as executor:
        with caplog.at_level("INFO"):
            exit_code = await executor.execute_job(
                arguments=["this is a test message"],
                **block.get_job_variables(),
            )

    assert exit_code == 0
    assert any("this is a test message" in rec.message for rec in caplog.records)


async def test_timeout(tmp_path):
    """Test timout error behavior. It must raise TimeoutError."""
    block = MiyabiJobBlock(
        work_root=str(tmp_path),
        executable="sleep",
        executor="local",
        launcher="single",
    )

    with pytest.raises(TimeoutError):
        with block.get_executor() as executor:
            await executor.execute_job(
                arguments=[3],
                **block.get_job_variables(),
                timeout=1,
            )

    # Don't delete the work directory when exception raised
    assert os.path.exists(executor.work_dir)
