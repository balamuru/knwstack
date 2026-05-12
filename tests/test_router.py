import pytest
from knwstack.api.decorators import reflex_rule, strategic_prompt, tactical_model, registry

def test_registry():
    """Verify that the decorators correctly registered the paths."""
    
    # Define rules inside the test so they register after the autouse clear() fixture
    @reflex_rule("weather.temp")
    def shutdown_reflex(events):
        return {"action": "SHUTDOWN"}

    @tactical_model("weather.wind")
    def wind_tactical(events):
        return {"action": "WARN"}

    @strategic_prompt("finance.tick")
    def analyze_market_anomaly(events):
        return {"messages": []}

    # Now check the registry
    assert len(registry.reflex_rules) == 1
    assert registry.reflex_rules[0]["topic"] == "weather.temp"
    
    assert len(registry.tactical_models) == 1
    assert registry.tactical_models[0]["topic"] == "weather.wind"
    
    assert len(registry.strategic_prompts) == 1
    assert registry.strategic_prompts[0]["topic"] == "finance.tick"
