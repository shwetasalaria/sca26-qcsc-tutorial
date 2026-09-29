from pathlib import Path

import pytest


@pytest.fixture
def reference_dir(request):
    return Path(request.fspath).parent / "reference"
