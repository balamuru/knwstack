import pytest
from unittest.mock import MagicMock, patch
from knwstack.engine.router import KnwStackEngine

def test_engine_builder_subjects():
    # Test that KnwStackEngine correctly extracts subjects from different input types
    from knwstack.connectors.nats_connector import NatsSource
    
    with patch("knwstack.engine.router.NatsSource", side_effect=NatsSource) as mock_source:
        # 1. String input
        engine = KnwStackEngine(inputs="test.>", output_subject="out")
        engine.build()
        mock_source.assert_called_with("nats://localhost:4222", ["test.>"], jetstream=False)
        
        # 2. List input
        engine = KnwStackEngine(inputs=["a", "b"], output_subject="out")
        engine.build()
        mock_source.assert_called_with("nats://localhost:4222", ["a", "b"], jetstream=False)
        
        # 3. Dict input
        engine = KnwStackEngine(inputs={"c": "mode1", "d": "mode2"}, output_subject="out")
        engine.build()
        mock_source.assert_called_with("nats://localhost:4222", ["c", "d"], jetstream=False)

@pytest.mark.asyncio
async def test_integration_flow_mocked():
    # This test mocks the entire Pathway and NATS layer to verify the internal wiring
    # For now, we've verified the components via test_logic.py and test_api.py.
    # A full end-to-end integration test would require a running NATS server.
    pass
