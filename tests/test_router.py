import pytest
import asyncio
from knwstack.api.decorators import reflex_rule, strategic_prompt, tactical_model, registry

# 1. Define dummy developers rules using the API
@reflex_rule("weather.temp")
def shutdown_reflex(events):
    # Rule: If temperature > 100 within the window, shut down immediately.
    for topic, data in events:
        if topic == "weather.temp" and data.get("temp", 0) > 100:
            return {"action": "SHUTDOWN_HARD", "reason": "overtemp"}
    return None

@tactical_model("weather.wind")
def wind_tactical(events):
    # Rule: Quick local heuristic classification
    for topic, data in events:
        if topic == "weather.wind" and data.get("speed", 0) > 50:
            return {"action": "WARN_WIND", "severity": "high"}
    return None

@strategic_prompt("finance.tick")
def analyze_market_anomaly(events):
    # Rule: If a huge market tick happens, ask the LLM for a strategic plan.
    messages = []
    for topic, data in events:
         if topic == "finance.tick" and data.get("price_drop", 0) > 10:
             messages.append({
                 "role": "user", 
                 "content": f"The market dropped {data['price_drop']}% instantly. What is the standard protocol?"
             })
             
    if not messages:
        return None
        
    return {
        "model": "gpt-3.5-turbo",
        "messages": messages
    }

def test_registry():
    """Verify that the decorators correctly registered the paths."""
    assert len(registry.reflex_rules) == 1
    assert registry.reflex_rules[0]["topic"] == "weather.temp"
    
    assert len(registry.tactical_models) == 1
    assert registry.tactical_models[0]["topic"] == "weather.wind"
    
    assert len(registry.strategic_prompts) == 1
    assert registry.strategic_prompts[0]["topic"] == "finance.tick"
