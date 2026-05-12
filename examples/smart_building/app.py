import logging
from knwstack.api.decorators import reflex_rule, tactical_model, strategic_prompt
from knwstack.engine.router import build_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ==========================================
# HOT PATH: Reflex Rules (< 10ms)
# ==========================================
@reflex_rule("bldg1.hvac.alarm")
def fire_alarm_reflex(events):
    """Instantly shuts off HVAC if a fire alarm is detected to prevent smoke spread."""
    for topic, data in events:
        if topic == "bldg1.hvac.alarm":
            if data.get("type") == "fire":
                logger.error("🚨 FIRE ALARM DETECTED! Executing reflex action: SHUTDOWN HVAC")
                return {"action": "shutdown", "reason": "fire_alarm", "building": "bldg1"}
    return None

# ==========================================
# WARM PATH: Tactical Models (< 100ms)
# ==========================================
@tactical_model("bldg1.hvac.telemetry", window_type="sliding", length_s=5, slide_s=1)
def temperature_tactical(events):
    """Calculates rolling averages over a 5-second sliding window."""
    temps = []
    for topic, data in events:
        if topic == "bldg1.hvac.telemetry":
            temps.append(data.get("temperature", 22.0))
            
    if len(temps) > 0:
        avg_temp = sum(temps) / len(temps)
        if avg_temp > 28.0:
            logger.warning(f"⚠️ High average temperature detected ({avg_temp:.1f}°C). Increasing cooling.")
            return {"action": "set_cooling", "value": "high", "avg_temp": avg_temp, "building": "bldg1"}
            
    return None

# ==========================================
# COLD PATH: Strategic Prompts (Seconds)
# ==========================================
@strategic_prompt("bldg1.hvac.telemetry", cooldown_s=60, window_type="tumbling", length_s=10)
def anomaly_strategic(events):
    """Uses an LLM to analyze complex anomalies (e.g., high power draw despite low temp)."""
    messages = []
    anomalies = []
    
    for topic, data in events:
        if topic == "bldg1.hvac.telemetry":
            if data.get("power_draw_kw", 0) > 10.0 and data.get("temperature", 100) < 20.0:
                anomalies.append(data)
                
    if not anomalies:
        return None
        
    logger.info("🧠 Anomalous power/temp correlation detected. Dispatching to LLM for strategic analysis.")
    messages.append({
        "role": "user",
        "content": f"The HVAC system is drawing over 10kW of power, but the room temperature is quite low. Here is the recent telemetry data: {anomalies}. What could cause this mechanical failure? Provide a short 2 sentence diagnosis."
    })
    
    return {
        "model": "gpt-4o-mini",
        "messages": messages
    }

@strategic_prompt("bldg1.hvac.alarm", cooldown_s=5)
def fire_analysis_strategic(events):
    """Uses an LLM to analyze the cause of a fire alarm for post-mortem reporting."""
    for topic, data in events:
        if topic == "bldg1.hvac.alarm" and data.get("type") == "fire":
            logger.info("🧠 Fire alarm detected. Dispatching for post-mortem AI analysis.")
            return {
                "model": "gpt-4o-mini",
                "messages": [
                    {"role": "system", "content": "You are a smart building safety inspector."},
                    {"role": "user", "content": f"A fire alarm was just triggered in the {data.get('zone', 'unknown')} zone. The reflex system has already shut down the HVAC. Please provide a brief (1-2 sentence) safety assessment for the maintenance crew."}
                ]
            }
    return None

# Engine Initialization
# ==========================================
engine = build_engine(
    nats_url="nats://localhost:4222", 
    inputs=["bldg1.hvac.alarm", "bldg1.hvac.telemetry"],
    output_subject="bldg1.actions"
)

if __name__ == "__main__":
    try:
        engine.run()
    except KeyboardInterrupt:
        logger.info("Shutting down KnwStack Engine...")
