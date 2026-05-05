import asyncio
import json
import random
import argparse
from nats.aio.client import Client as NATS

async def run(mode):
    nc = NATS()
    await nc.connect("nats://localhost:4222")

    if mode == "telemetry_normal":
        print("Publishing normal telemetry...")
        for _ in range(5):
            payload = {
                "temperature": random.uniform(21.0, 23.0),
                "power_draw_kw": random.uniform(2.0, 3.5),
                "humidity": 45.0
            }
            await nc.publish("bldg1.hvac.telemetry", json.dumps(payload).encode())
            await asyncio.sleep(0.1)

    elif mode == "telemetry_hot":
        print("Publishing HOT telemetry (Triggers WARM PATH)...")
        for _ in range(5):
            payload = {
                "temperature": random.uniform(29.0, 31.0),
                "power_draw_kw": random.uniform(4.0, 5.5),
                "humidity": 45.0
            }
            await nc.publish("bldg1.hvac.telemetry", json.dumps(payload).encode())
            await asyncio.sleep(0.1)

    elif mode == "anomaly":
        print("Publishing mechanical anomaly (Triggers COLD PATH)...")
        for _ in range(5):
            payload = {
                "temperature": random.uniform(18.0, 19.5), # Low temp
                "power_draw_kw": random.uniform(11.0, 14.0), # High power
                "humidity": 45.0
            }
            await nc.publish("bldg1.hvac.telemetry", json.dumps(payload).encode())
            await asyncio.sleep(0.1)

    elif mode == "fire":
        print("Publishing fire alarm (Triggers HOT PATH)...")
        payload = {"type": "fire", "zone": "lobby"}
        await nc.publish("bldg1.hvac.alarm", json.dumps(payload).encode())
        
    print("Done!")
    await nc.drain()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Smart Building Telemetry Generator')
    parser.add_argument('--mode', choices=['telemetry_normal', 'telemetry_hot', 'anomaly', 'fire'], required=True)
    args = parser.parse_args()
    
    asyncio.run(run(args.mode))
