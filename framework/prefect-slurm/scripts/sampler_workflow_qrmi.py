import asyncio
import json
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
from qiskit_ibm_runtime.utils import RuntimeEncoder
from qiskit_ibm_runtime.utils.result_decoder import ResultDecoder

BITLEN = 10

@flow(name="slurm_tutorial")
async def main():
    # Load configurations
    runtime = await QuantumRuntime.load("ibm-runner")
    counter = await BitCounter.load("slurm-tutorial")
    options = await Variable.get("slurm-tutorial")
    taskrunner = await TaskRunner.load("slurm-tutorial")

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

    # Extract shots
    shots = options.get("shots", 100000) # default to 100000 if not set

    dict_pubs = []

    # Create input.json for task_runner
    coerced_pub = SamplerPub.coerce(pub_like, shots=shots)

    # Generate OpenQASM3 string which can be consumed by IBM Quantum APIs
    qasm3_str = qasm3.dumps(
            coerced_pub.circuit,
            disable_constants=True,
            allow_aliasing=True,
            experimental=qasm3.ExperimentalFeatures.SWITCH_CASE_V1,
    )

    if len(coerced_pub.circuit.parameters) == 0:
        if coerced_pub.shots:
            dict_pubs.append((qasm3_str, None, coerced_pub.shots))
        else:
            dict_pubs.append((qasm3_str))
    else:
        param_array = coerced_pub.parameter_values.as_array(
        coerced_pub.circuit.parameters
                ).tolist()

        if coerced_pub.shots:
            dict_pubs.append((qasm3_str, param_array, coerced_pub.shots))
        else:
            dict_pubs.append((qasm3_str, param_array))

    # Create SamplerV2 input
    input_json = {
            "pubs": dict_pubs,
            "shots": shots,
            "options": options,
            "version": 2,
            "support_qiskit": True,
    }
    print("Here is the input json:", input_json)

    taskrunner_json = {"parameters": input_json, "program_id": "sampler"}
    print("Here is the taskrunner json:", taskrunner_json)

    filename = "/mnt/data/salaria/github/qii-miyabi-kawasaki/framework/prefect-slurm/input.json"
    with open(filename, "w", encoding="utf-8") as primitive_input_file:
        json.dump(taskrunner_json, primitive_input_file, cls=RuntimeEncoder, indent=2)

    # Quantum execution
    result = await taskrunner.run()
    
    # Read output
    with open('/mnt/data/salaria/github/qii-miyabi-kawasaki/framework/prefect-slurm/output.json', 'r') as f:
        results = ResultDecoder.decode(f.read())

    # MPI execution
    bitstrings = results[0].data.meas.get_bitstrings()
    counts = await counter.get(bitstrings)

    # Save in Prefect artifacts
    await create_table_artifact(
        table=[list(counts.keys()), list(counts.values())],
        key="sampler-count-dict",
     )


if __name__ == "__main__":
    asyncio.run(main())

