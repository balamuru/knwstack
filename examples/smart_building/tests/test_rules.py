import pytest
import sys
import os

# Add parent directory to sys.path so we can import app.py
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import fire_alarm_reflex, temperature_tactical, anomaly_strategic

def test_fire_alarm_reflex_detects_fire():
    """Verify that fire alarm reflex triggers shutdown."""
    events = [("bldg1.hvac.alarm", {"type": "fire"})]
    result = fire_alarm_reflex(events)
    assert result is not None
    assert result["action"] == "shutdown"
    assert result["building"] == "bldg1"

def test_fire_alarm_reflex_ignores_others():
    """Verify that non-fire alarms are ignored by the reflex rule."""
    events = [("bldg1.hvac.alarm", {"type": "security_breach"})]
    result = fire_alarm_reflex(events)
    assert result is None

def test_temperature_tactical_averaging():
    """Verify that tactical rule correctly averages temperatures and triggers cooling."""
    events = [
        ("bldg1.hvac.telemetry", {"temperature": 29.0, "key": "bldg1"}),
        ("bldg1.hvac.telemetry", {"temperature": 31.0, "key": "bldg1"})
    ]
    result = temperature_tactical(events)
    assert result is not None
    assert result["action"] == "set_cooling"
    assert result["avg_temp"] == 30.0

def test_temperature_tactical_nominal():
    """Verify that tactical rule stays silent for nominal temperatures."""
    events = [
        ("bldg1.hvac.telemetry", {"temperature": 22.0, "key": "bldg1"}),
        ("bldg1.hvac.telemetry", {"temperature": 24.0, "key": "bldg1"})
    ]
    result = temperature_tactical(events)
    assert result is None

def test_anomaly_strategic_detection():
    """Verify that strategic rule detects high power/low temp correlation."""
    events = [
        ("bldg1.hvac.telemetry", {"power_draw_kw": 15.0, "temperature": 15.0, "key": "bldg1"})
    ]
    result = anomaly_strategic(events)
    assert result is not None
    assert "messages" in result
    assert "gpt-4o-mini" in result["model"]

if __name__ == "__main__":
    pytest.main([__file__])
