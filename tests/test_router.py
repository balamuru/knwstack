import pytest
from knwstack.api.decorators import reflex_rule, strategic_prompt, tactical_model, registry
from knwstack.engine.router import get_key, match_subject

def test_registry_registration():
    """Verify that the decorators correctly registered the paths."""
    @reflex_rule("test.reflex")
    def my_reflex(events): return {}

    @tactical_model("test.tactical")
    def my_tactical(events): return {}
    
    assert len(registry.reflex_rules) >= 1
    assert registry.reflex_rules[-1]["topic"] == "test.reflex"
    assert len(registry.tactical_models) >= 1
    assert registry.tactical_models[-1]["topic"] == "test.tactical"

def test_strict_key_extraction():
    """Verify that the engine enforces strict key extraction."""
    # 1. Valid key in data
    assert get_key({"key": "bldg1", "temp": 20}, "subject") == "bldg1"
    
    # 2. Missing key should return 'unknown' (NOT the subject)
    assert get_key({"temp": 20}, "bldg1.telemetry") == "unknown"
    
    # 3. Key in nested data (unsupported for now, should be top level)
    assert get_key({"metadata": {"key": "bldg1"}}, "subject") == "unknown"

def test_match_subject_logic():
    """Verify subject matching patterns."""
    assert match_subject("bldg1.>", "bldg1.temp") is True
    assert match_subject("bldg1.>", "bldg2.temp") is False
    assert match_subject("*.temp", "bldg1.temp") is True
    assert match_subject("*.temp", "bldg1.humidity") is False
    assert match_subject(">", "anything") is True
