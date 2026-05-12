import asyncio
import json
import random
import argparse
import sys
from nats.aio.client import Client as NATS

async def dispatch_telemetry(nc):
    print("🟢 Dispatching: TELEMETRY (Nominal Stream)")
    for _ in range(5):
        payload = {
            "temperature": random.uniform(21.0, 23.0),
            "power_draw_kw": random.uniform(2.0, 3.5),
            "humidity": 45.0
        }
        await nc.publish("bldg1.hvac.telemetry", json.dumps(payload).encode())
        await asyncio.sleep(0.1)

async def dispatch_high_temp(nc):
    print("🟠 Dispatching: HIGH_TEMP (Tactical Path)")
    for _ in range(5):
        payload = {
            "temperature": random.uniform(29.0, 31.0),
            "power_draw_kw": random.uniform(4.0, 5.5),
            "humidity": 45.0
        }
        await nc.publish("bldg1.hvac.telemetry", json.dumps(payload).encode())
        await asyncio.sleep(0.1)

async def dispatch_anomaly(nc):
    print("🔵 Dispatching: ANOMALY (Strategic Path)")
    for _ in range(5):
        payload = {
            "temperature": random.uniform(18.0, 19.5), # Low temp
            "power_draw_kw": random.uniform(11.0, 14.0), # High power mismatch
            "humidity": 45.0
        }
        await nc.publish("bldg1.hvac.telemetry", json.dumps(payload).encode())
        await asyncio.sleep(0.1)

async def dispatch_fire_alarm(nc):
    print("🔴 Dispatching: FIRE_ALARM (Hot Path)")
    payload = {"type": "fire", "zone": "lobby"}
    await nc.publish("bldg1.hvac.alarm", json.dumps(payload).encode())

async def run_interactive(nc):
    while True:
        print("\n" + "="*45)
        print("    KnwStack Smart Building Event Generator")
        print("="*45)
        print("  1. 🟢 Nominal Telemetry (Reliable)")
        print("  2. 🔴 Fire Alarm        (Hot Reflex)")
        print("  3. 🟠 High Temp         (Warm Tactical)")
        print("  4. 🔵 Anomaly           (Cold Strategic)")
        print("  5. ❌ Exit")
        print("="*45)
        
        try:
            choice = await asyncio.get_event_loop().run_in_executor(None, input, "Select option: ")
            if choice == "1":
                await dispatch_telemetry(nc)
            elif choice == "2":
                await dispatch_fire_alarm(nc)
            elif choice == "3":
                await dispatch_high_temp(nc)
            elif choice == "4":
                await dispatch_anomaly(nc)
            elif choice == "5" or choice.lower() == 'q':
                break
            else:
                print("⚠️ Invalid choice.")
            
            print("✅ Dispatch complete.")
        except EOFError:
            break
        except Exception as e:
            print(f"❌ Error: {e}")

async def main():
    parser = argparse.ArgumentParser(
        description='KnwStack Smart Building Telemetry Generator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Interactive Mode (Default):
  Run without arguments to open the color-coded menu.
  This is the recommended way to test multiple scenarios sequentially.

Single-Shot Modes:
  telemetry   -> Nominal data stream.
  fire_alarm  -> Triggers HOT PATH (Immediate Reflex).
  high_temp   -> Triggers WARM PATH (Tactical CEP).
  anomaly     -> Triggers COLD PATH (Strategic AI).
"""
    )
    parser.add_argument('--mode', 
                        choices=['telemetry', 'high_temp', 'anomaly', 'fire_alarm'], 
                        help="Single-shot mode. If omitted, starts interactive menu.")
    args = parser.parse_args()

    nc = NATS()
    try:
        await nc.connect("nats://localhost:4222")
    except Exception as e:
        print(f"❌ Failed to connect to NATS: {e}")
        return

    if args.mode:
        if args.mode == "telemetry":
            await dispatch_telemetry(nc)
        elif args.mode == "high_temp":
            await dispatch_high_temp(nc)
        elif args.mode == "anomaly":
            await dispatch_anomaly(nc)
        elif args.mode == "fire_alarm":
            await dispatch_fire_alarm(nc)
        print("✅ Dispatch complete.")
    else:
        await run_interactive(nc)

    await nc.drain()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Exiting generator...")
