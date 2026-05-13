import logging
import argparse
from knwstack.api.decorators import reflex_rule, tactical_model, strategic_prompt
from knwstack.engine.router import build_engine

def setup_logging(level_name):
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(message)s")
    # Also set level for the core knwstack logger if it differs
    logging.getLogger("knwstack").setLevel(level)

logger = logging.getLogger(__name__)

# ==========================================
# HOT PATH: Reflex Rules (< 10ms)
# ==========================================
@reflex_rule(">.alarm")
def fire_alarm_reflex(events):
    """Instantly shuts off HVAC if a fire alarm is detected."""
    for topic, data in events:
        building = topic.split(".")[0]
        if data.get("type") == "fire":
            logger.error(f"🚨 FIRE ALARM DETECTED in {building}! Executing reflex action: SHUTDOWN HVAC")
            return {"action": "shutdown", "reason": "fire_alarm", "building": building}
    return None

# ==========================================
# WARM PATH: Tactical Models (< 100ms)
# ==========================================
@tactical_model(">.telemetry", window_type="sliding", length_s=5, slide_s=1)
def temperature_tactical(events):
    """Calculates rolling averages over a 5-second sliding window."""
    temps = []
    building = "unknown"
    for topic, data in events:
        if "temperature" not in data: continue
        building = data.get("key", topic.split(".")[0])
        temps.append(data.get("temperature", 22.0))
        
    if len(temps) > 0:
        avg_temp = sum(temps) / len(temps)
        if avg_temp > 28.0:
            logger.warning(f"⚠️ [WARM] High average temperature detected in {building} ({avg_temp:.1f}°C) over {len(temps)} samples. Increasing cooling.")
            return {"action": "set_cooling", "value": "high", "avg_temp": avg_temp, "building": building}
            
    return None

# ==========================================
# COLD PATH: Strategic Prompts (Seconds)
# ==========================================
@strategic_prompt(">.telemetry", cooldown_s=60, window_type="tumbling", length_s=10)
def anomaly_strategic(events):
    """Uses an LLM to analyze complex anomalies."""
    anomalies = []
    building = "unknown"
    
    for topic, data in events:
        if "power_draw_kw" not in data: continue
        building = data.get("key", topic.split(".")[0])
        if data.get("power_draw_kw", 0) > 10.0 and data.get("temperature", 100) < 20.0:
            anomalies.append(data)
                
    if not anomalies:
        return None
        
    logger.info(f"🧠 Anomalous power/temp correlation detected in {building}. Dispatching to LLM.")
    messages = [
        {"role": "system", "content": "You are a concise smart building analyst. Provide a 1-2 sentence technical diagnosis of anomalies."},
        {"role": "user", "content": f"The HVAC system in {building} is drawing over 10kW of power, but the room temperature is quite low. Telemetry: {anomalies}. Diagnosis?"}
    ]
    
    return {
        "model": "gpt-4o-mini",
        "messages": messages
    }

@strategic_prompt(">.alarm", cooldown_s=5)
def fire_analysis_strategic(events):
    """Uses an LLM to analyze the cause of a fire alarm."""
    for topic, data in events:
        building = topic.split(".")[0]
        if data.get("type") == "fire":
            logger.info(f"🧠 Fire alarm detected in {building}. Dispatching for post-mortem AI analysis.")
            return {
                "model": "gpt-4o-mini",
                "messages": [
                    {"role": "system", "content": "You are a concise smart building safety inspector. Provide a 1-2 sentence assessment."},
                    {"role": "user", "content": f"A fire alarm was triggered in {building}. The system has shut down the HVAC. Brief assessment?"}
                ]
            }
    return None

# Engine Initialization
# ==========================================
engine = build_engine(
    nats_url="nats://localhost:4222", 
    inputs=[">"],
    output_subject="campus.actions"
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KnwStack Smart Building Example")
    parser.add_argument("--log", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO", help="Set the logging level (default: INFO)")
    args = parser.parse_args()
    
    setup_logging(args.log)
    
    try:
        engine.run()
    except KeyboardInterrupt:
        logger.info("Shutting down KnwStack Engine...")
