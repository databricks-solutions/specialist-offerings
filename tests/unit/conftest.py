import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "conf" / "examples"


@pytest.fixture
def examples():
    return EXAMPLES
