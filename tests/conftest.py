import pytest
from knwstack.api.decorators import registry

@pytest.fixture(autouse=True)
def clear_registry():
    """Ensure every test starts with a clean registry."""
    registry.clear()
    yield
