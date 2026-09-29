"""Test for job script generation."""

import pytest

from prefect_miyabi.core import MiyabiJobBlock

cases = [
    pytest.param(
        {
            "launcher": "single",
            "queue_name": "debug-c",
            "project": "group1",
            "num_nodes": 1,
            "walltime": "30:00",
        },
        None,
        id="case01",
    ),
    pytest.param(
        {
            "launcher": "single",
            "queue_name": "debug-c",
            "project": "group1",
            "num_nodes": 1,
            "walltime": "30:00",
        },
        ["-x", "123", "-y", "456"],
        id="case02",
    ),
    pytest.param(
        {
            "launcher": "single",
            "queue_name": "debug-c",
            "project": "group1",
            "num_nodes": 1,
            "ompthreads": 64,
            "walltime": "01:00:00",
        },
        None,
        id="case03",
    ),
    pytest.param(
        {
            "launcher": "mpiexec.hydra",
            "queue_name": "debug-c",
            "project": "group1",
            "num_nodes": 1,
            "mpiprocs": 112,
            "walltime": "15:00",
        },
        None,
        id="case04",
    ),
    pytest.param(
        {
            "launcher": "mpiexec.hydra",
            "queue_name": "debug-c",
            "project": "group1",
            "num_nodes": 1,
            "mpiprocs": 112,
            "walltime": "15:00",
        },
        ["-x", "123", "-y", "456"],
        id="case05",
    ),
    pytest.param(
        {
            "launcher": "mpiexec.hydra",
            "queue_name": "debug-c",
            "project": "group1",
            "num_nodes": 1,
            "mpiprocs": 10,
            "mpi_options": ["-np", "30", "--oversubscribe"],
            "walltime": "15:00",
        },
        ["-x", "123", "-y", "456"],
        id="case06",
    ),
    pytest.param(
        {
            "launcher": "mpiexec.hydra",
            "queue_name": "short-c",
            "project": "group1",
            "num_nodes": 1,
            "mpiprocs": 4,
            "ompthreads": 8,
            "walltime": "04:00:00",
        },
        None,
        id="case07",
    ),
    pytest.param(
        {
            "launcher": "mpiexec.hydra",
            "queue_name": "regular-c",
            "project": "group1",
            "num_nodes": 2,
            "mpiprocs": 48,
            "walltime": "12:00:00",
        },
        None,
        id="case08",
    ),
    pytest.param(
        {
            "launcher": "mpiexec.hydra",
            "queue_name": "regular-c",
            "project": "group1",
            "num_nodes": 2,
            "mpiprocs": 4,
            "ompthreads": 12,
            "walltime": "24:00:00",
        },
        None,
        id="case09",
    ),
    pytest.param(
        {
            "launcher": "mpiexec.hydra",
            "queue_name": "debug-c",
            "project": "group1",
            "num_nodes": 1,
            "mpiprocs": 4,
            "environments": {
                "TEST1": "xyz",
                "TEST2": "abc",
            },
        },
        None,
        id="case10",
    ),
    pytest.param(
        {
            "launcher": "mpiexec.hydra",
            "queue_name": "short-c",
            "project": "group1",
            "num_nodes": 1,
            "mpiprocs": 10,
            "modules": ["module_x", "module_y", "module_z"],
        },
        None,
        id="case11",
    ),
    pytest.param(
        {
            "launcher": "mpiexec.hydra",
            "queue_name": "large-c",
            "project": "group1",
            "num_nodes": 30,
            "mpiprocs": 10,
            "mpi_options": ["-np", "300"],
            "ompthreads": 10,
            "modules": ["module_x", "module_y", "module_z"],
            "environments": {
                "TEST1": "xyz",
                "TEST2": "abc",
            },
        },
        ["-foo", "123", "-bar", "/path/to/some/file"],
        id="case12",
    ),
]


@pytest.mark.parametrize("params, arguments", cases)
async def test_pbs(params, arguments, tmp_path, reference_dir, request):
    """Test PBS batch job script generation with various job parameters."""
    current_id = request.node.callspec.id

    block = MiyabiJobBlock(
        work_root=str(tmp_path),
        executable="/path/to/executable/a.out",
        executor="pbs",
        **params,
    )
    with block.get_executor() as executor:
        job_vars = block.get_job_variables()
        if arguments:
            job_vars["arguments"] = arguments
        file = executor.write_job_script(**job_vars)
        with open(file) as fp:
            test_script = fp.read().replace(str(executor.work_dir), "<TMPDIR>")
        with open(reference_dir / f"batch_{current_id}.pbs") as fp:
            ref_script = fp.read()
        assert test_script == ref_script


@pytest.mark.parametrize("params, arguments", cases)
async def test_local(params, arguments, tmp_path, reference_dir, request):
    """Test shell script generation with various job parameters."""
    current_id = request.node.callspec.id

    block = MiyabiJobBlock(
        work_root=str(tmp_path),
        executable="/path/to/executable/a.out",
        executor="local",
        **params,
    )
    with block.get_executor() as executor:
        job_vars = block.get_job_variables()
        if arguments:
            job_vars["arguments"] = arguments
        file = executor.write_job_script(**job_vars)
        with open(file) as fp:
            test_script = fp.read().replace(str(executor.work_dir), "<TMPDIR>")
        with open(reference_dir / f"local_{current_id}.sh") as fp:
            ref_script = fp.read()
        assert test_script == ref_script
