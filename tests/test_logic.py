import pytest
from knwstack.engine.router import apply_reflex, run_tactical, run_strategic

def test_apply_reflex_trigger():
    def mock_rule(events):
        return {"action": "fire"}
    
    rules = [{"topic": "alarm", "func": mock_rule}]
    result = apply_reflex("alarm", {"type": "fire"}, rules, "output")
    
    assert result["subject"] == "output.reflex"
    assert result["data"] == {"action": "fire"}

def test_apply_reflex_no_match():
    def mock_rule(events):
        return {"action": "fire"}
    
    rules = [{"topic": "alarm", "func": mock_rule}]
    result = apply_reflex("telemetry", {"temp": 20}, rules, "output")
    
    assert result == {}

def test_run_tactical_logic():
    def mock_model(events):
        # Average temperature
        temps = [e[1]["temp"] for e in events]
        return {"avg": sum(temps) / len(temps)}
    
    cfg = {"func": mock_model}
    events = [
        {"temp": 10},
        {"temp": 20}
    ]
    
    result = run_tactical(events, "temp", cfg, "output")
    assert result["data"]["avg"] == 15

@pytest.mark.asyncio
async def test_run_strategic_logic_no_anomaly():
    def mock_prompt(events):
        return None # No anomaly
    
    cfg = {"func": mock_prompt}
    events = [("temp", {"temp": 10})]
    
    result = await run_strategic(events, "temp", cfg, "output")
    assert result == {}
