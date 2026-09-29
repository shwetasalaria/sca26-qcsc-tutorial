import os

from prefect.blocks.core import Block
from pydantic import Field, SecretStr

from qrmi import QuantumResource, ResourceType
from qrmi.primitives.ibm import get_target


class QRMIResource(Block):
    """
    Prefect block for configuring and accessing a QRMI resource.
    """

    _block_type_name = "QRMI Resource"
    _block_type_slug = "qrmi-resource"

    resource_id: str = Field(
        description="QRMI resource ID, e.g. test_heron"
    )

    endpoint: str = Field(
        description="IBM Quantum System endpoint"
    )

    iam_endpoint: str = Field(
        description="IBM IAM endpoint"
    )

    api_key: SecretStr = Field(
        description="IBM Quantum API key"
    )

    service_crn: SecretStr = Field(
        description="IBM Quantum service CRN"
    )

    async def get_target(self):
        """Get the Qiskit Target for the QRMI resource."""

        prefix = f"{self.resource_id}_QRMI_IBM_QS"

        os.environ[f"{prefix}_ENDPOINT"] = self.endpoint
        os.environ[f"{prefix}_IAM_ENDPOINT"] = self.iam_endpoint
        os.environ[f"{prefix}_IAM_APIKEY"] = self.api_key.get_secret_value()
        os.environ[f"{prefix}_SERVICE_CRN"] = self.service_crn.get_secret_value()

        resource = QuantumResource(
            self.resource_id,
            ResourceType.IBMQuantumSystem,
        )

        return get_target(resource)
