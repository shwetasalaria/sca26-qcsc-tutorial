import asyncio
from prefect import flow
from prefect_slurm import SlurmJobBlock

@flow
async def say_hello_slurm():
    # Define an HPC job configuration
    block = SlurmJobBlock(
        job_name = "test",
        work_root="/mnt/data/salaria/test-dir",
        executable="/bin/echo",
        executor="sbatch",
        launcher="single",
        partition="compute",
        num_nodes=1,
        walltime="00:05:00",
    )
    # Execute with provided settings
    with block.get_executor() as executor:
        await executor.execute_job(
            arguments=["Hello Slurm Cluster!"],
            **block.get_job_variables(),
        )

if __name__ == "__main__":
    asyncio.run(say_hello_slurm())
