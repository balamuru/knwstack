import asyncio
import json
import random
import argparse
from nats.aio.client import Client as NATS

async def run(mode):
    nc = NATS()
    await nc.connect("nats://localhost:4222")

    if mode == "telemetry":
        print("🟢 Mode: TELEMETRY (Nominal Stream)")
        for _ in range(5):
            payload = {
                "temperature": random.uniform(21.0, 23.0),
                "power_draw_kw": random.uniform(2.0, 3.5),
                "humidity": 45.0
            }
            await nc.publish("bldg1.hvac.telemetry", json.dumps(payload).encode())
            await asyncio.sleep(0.1)

    elif mode == "high_temp":
        print("🟠 Mode: HIGH_TEMP (Tactical Path)")
        for _ in range(5):
            payload = {
                "temperature": random.uniform(29.0, 31.0),
                "power_draw_kw": random.uniform(4.0, 5.5),
                "humidity": 45.0
            }
            await nc.publish("bldg1.hvac.telemetry", json.dumps(payload).encode())
            await asyncio.sleep(0.1)

    elif mode == "anomaly":
        print("🔵 Mode: ANOMALY (Strategic Path)")
        for _ in range(5):
            payload = {
                "temperature": random.uniform(18.0, 19.5), # Low temp
                "power_draw_kw": random.uniform(11.0, 14.0), # High power mismatch
                "humidity": 45.0
            }
            await nc.publish("bldg1.hvac.telemetry", json.dumps(payload).encode())
            await asyncio.sleep(0.1)

    elif mode == "fire_alarm":
        print("🔴 Mode: FIRE_ALARM (Hot Path)")
        payload = {"type": "fire", "zone": "lobby"}
        await nc.publish("bldg1.hvac.alarm", json.dumps(payload).encode())
        
    print("✅ Dispatch complete.")
    await nc.drain()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='KnwStack Smart Building Telemetry Generator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Mode Definitions:
  telemetry   -> Nominal data stream. [Transport: Reliable (Pull)]
  high_temp   -> Triggers WARM PATH (Tactical CEP). [Transport: Reliable (Pull)]
  anomaly     -> Triggers COLD PATH (Strategic AI). [Transport: Reliable (Pull)]
  fire_alarm  -> Triggers HOT PATH (Immediate Reflex). [Transport: SuperHot (Push)]
"""
    )
    parser.add_argument('--mode', 
                        choices=['telemetry', 'high_temp', 'anomaly', 'fire_alarm'], 
                        required=True,
                        help="The type of event to generate (see definitions below)")
    args = parser.parse_args()
    
    asyncio.run(run(args.mode))
