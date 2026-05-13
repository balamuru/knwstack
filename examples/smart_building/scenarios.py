import json
import time
import nats
import asyncio
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tester")

async def test():
    nc = await nats.connect("nats://localhost:4222")
    
    logger.info("🚀 Starting Automated Scenario Verification...")

    # --- Scenario 1: Fire Alarm (Reflex) ---
    logger.info("🔥 SCENARIO 1: Dispatching Fire Alarm to bldg1.alarm")
    await nc.publish("bldg1.alarm", json.dumps({"type": "fire", "key": "bldg1"}).encode())
    
    # --- Scenario 2: High Temp (Tactical) ---
    logger.info("🟠 SCENARIO 2: Dispatching High Temp (32°C) to bldg1.telemetry (5 events)")
    for _ in range(5):
        await nc.publish("bldg1.telemetry", json.dumps({"temperature": 32.0, "key": "bldg1"}).encode())
    
    # --- Scenario 3: Anomaly (Strategic) ---
    logger.info("🔵 SCENARIO 3: Dispatching Anomaly (High Power + Low Temp) to bldg1.telemetry")
    payload = {"temperature": 15.0, "power_draw_kw": 12.5, "key": "bldg1"}
    await nc.publish("bldg1.telemetry", json.dumps(payload).encode())

    # --- Scenario 4: Campus Partitioning ---
    logger.info("🏢 SCENARIO 4: Dispatching Campus Simulation (Alpha: 22, Beta: 32, Gamma: 15)")
    # Alpha: Nominal (3 events)
    for _ in range(3):
        await nc.publish("campus.telemetry", json.dumps({"temperature": 22.0, "key": "bldg_alpha"}).encode())
    # Beta: Hot (3 events)
    for _ in range(3):
        await nc.publish("campus.telemetry", json.dumps({"temperature": 32.0, "key": "bldg_beta"}).encode())
    # Gamma: Cold (3 events)
    for _ in range(3):
        await nc.publish("campus.telemetry", json.dumps({"temperature": 15.0, "key": "bldg_gamma"}).encode())

    logger.info("✅ All test events dispatched. Monitoring responses in app.py logs...")
    await asyncio.sleep(5)
    await nc.close()

if __name__ == "__main__":
    asyncio.run(test())
