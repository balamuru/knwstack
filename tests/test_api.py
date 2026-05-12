import pytest
from knwstack.api.decorators import reflex_rule, tactical_model, strategic_prompt, registry

def test_reflex_decorator():
    @reflex_rule("test.reflex")
    def my_reflex(events):
        return {"action": "test"}
    
    # Check if it exists in registry
    matches = [r for r in registry.reflex_rules if r["topic"] == "test.reflex"]
    assert len(matches) == 1
    assert matches[0]["func"] == my_reflex.__wrapped__

def test_tactical_decorator():
    @tactical_model("test.tactical", window_type="sliding", length_s=10, slide_s=2)
    def my_tactical(events):
        return {"action": "test"}
    
    matches = [m for m in registry.tactical_models if m["topic"] == "test.tactical"]
    assert len(matches) == 1
    assert matches[0]["func"] == my_tactical.__wrapped__
    assert matches[-1]["window_type"] == "sliding"
    assert matches[-1]["length_s"] == 10
    assert matches[-1]["slide_s"] == 2

def test_strategic_decorator():
    @strategic_prompt("test.strategic", cooldown_s=30)
    def my_strategic(events):
        return {"messages": []}
    
    matches = [p for p in registry.strategic_prompts if p["topic"] == "test.strategic"]
    assert len(matches) == 1
    assert matches[0]["func"] == my_strategic.__wrapped__
    assert matches[-1]["cooldown_s"] == 30
