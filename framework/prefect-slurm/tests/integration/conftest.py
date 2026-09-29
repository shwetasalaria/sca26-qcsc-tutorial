from collections.abc import Generator

import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(autouse=True, scope="module")
def prefect_test_fixture() -> Generator[None, None, None]:
    """Fixture to start prefect server in test mode."""
    with prefect_test_harness():
        yield
