"""Test PyFunctionJob block."""

import os
import sys

import numpy as np

from prefect_miyabi.pyfunc import PyFunctionJob


def numpy_fn(arg1: np.ndarray, arg2: np.ndarray) -> np.ndarray:
    """A function to be tested that uses numpy ufunc feature."""
    bias = os.getenv("BIAS")
    if bias is None:
        raise RuntimeError("BIAS variable is not exported!")
    return arg1 + arg2 + float(bias)


async def test_pyfunc_numpy(tmp_path):
    """Test running Python function with local job executor."""
    rng = np.random.default_rng(827)
    bias = 0.12345

    func_job = PyFunctionJob(
        work_root=str(tmp_path),
        executable=sys.executable,  # Identical interpreter for testing.
        executor="local",
        launcher="single",
        environments={
            "BIAS": str(bias),
        },
    )

    vec1 = rng.random(100)
    vec2 = rng.random(100)

    # Run Python function in separate process
    job_ret = await func_job.run(numpy_fn, arg1=vec1, arg2=vec2)

    np.testing.assert_almost_equal(vec1 + vec2 + bias, job_ret)
