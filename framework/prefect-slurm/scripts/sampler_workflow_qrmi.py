import asyncio
import json, os
from prefect import flow
from prefect.artifacts import create_table_artifact
from prefect.variables import Variable
from prefect_qiskit import QuantumRuntime
from qiskit import QuantumCircuit
from qiskit.transpiler import generate_preset_pass_manager
from get_counts_integration import BitCounter
from qiskit.primitives.containers.sampler_pub import SamplerPub
from qiskit import qasm3
from get_task_runner import TaskRunner
from qiskit_ibm_runtime import RuntimeEncoder
from qiskit_ibm_runtime.decoders.result_decoder import ResultDecoder
from qrmi_resource import QRMIResource

BITLEN = 10

@flow(name="bit_count")
async def main():

    # Load configurations
    qrmi = await QRMIResource.load("ibm-quantum-credentials")
    counter = await BitCounter.load("bit-count")
    options = await Variable.get("bit-count")
    taskrunner = await TaskRunner.load("bit-count")

    # Create a PUB payload
    target = await qrmi.get_target()
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

    # Extract shots
    shots = options.get("shots", 100000) # default to 100000 if not set

    # Create input.json for task_runner
    coerced_pub = SamplerPub.coerce((isa,), shots=shots)

    # Generate OpenQASM3 string which can be consumed by IBM Quantum APIs
    qasm3_str = qasm3.dumps(
            coerced_pub.circuit,
            disable_constants=True,
            allow_aliasing=True,
            experimental=qasm3.ExperimentalFeatures.SWITCH_CASE_V1,
    )

    # Create SamplerV2 input
    input_json = {
    "pubs": [
        (qasm3_str, None, shots)
    ],
    "shots": shots,
    "options": {},
    "version": 2,
    "support_qiskit": False,
    }

    taskrunner_json = {"parameters": input_json, "program_id": "sampler"}

    filename = "/data/bit-count/input.json"
    with open(filename, "w", encoding="utf-8") as primitive_input_file:
        json.dump(taskrunner_json, primitive_input_file, indent=2)

    # Quantum execution
    result = await taskrunner.run()

    # Read output
    with open('/data/bit-count/output.json', 'r') as f:
        results = ResultDecoder.decode(f.read())

    # MPI execution
    samples = results["results"][0]["data"]["meas"]["samples"]
    num_bits = results["results"][0]["data"]["meas"]["num_bits"]
    bitstrings = [format(int(s, 16), f"0{num_bits}b") for s in samples]
    counts = await counter.get(bitstrings)

    # Save in Prefect artifacts
    await create_table_artifact(
        table=[list(counts.keys()), list(counts.values())],
        key="sampler-count-dict",
     )


if __name__ == "__main__":
    asyncio.run(main())
