import asyncio
from prefect import flow
from prefect.artifacts import create_table_artifact
from prefect.variables import Variable
from prefect_qiskit import QuantumRuntime
from qiskit import QuantumCircuit
from qiskit.transpiler import generate_preset_pass_manager
from get_counts_integration import BitCounter
from qiskit.primitives.containers.sampler_pub import SamplerPub
from qiskit import qasm3

BITLEN = 10


@flow(name="slurm_tutorial")
async def main():
    # Load configurations
    runtime = await QuantumRuntime.load("ibm-runner")
    counter = await BitCounter.load("slurm-tutorial")
    options = await Variable.get("slurm-tutorial")

    # Create a PUB payload
    target = await runtime.get_target()
    qc_ghz = QuantumCircuit(BITLEN)
    qc_ghz.h(0)
    qc_ghz.cx(0, range(1, BITLEN))
    qc_ghz.measure_active()

    pm = generate_preset_pass_manager(
        optimization_level=3,
        target=target,
        seed_transpiler=123,
    )
    isa = pm.run(qc_ghz)
    pub_like = (isa,)  # Create a Primitive Unified Bloc

    # Execute
    results = await runtime.sampler([pub_like], options=options)
    bitstrings = results[0].data.meas.get_bitstrings()
    counts = await counter.get(bitstrings)

    # Save in Prefect artifacts
    await create_table_artifact(
        table=[list(counts.keys()), list(counts.values())],
        key="sampler-count-dict",
    )


if __name__ == "__main__":
    asyncio.run(main())
