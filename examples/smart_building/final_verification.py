import asyncio
import os
import subprocess
import time

async def run_test(mode, label):
    print(f"\n🚀 Running Test: {label} ({mode})...")
    subprocess.run(["uv", "run", "python", "generator.py", "--mode", mode], check=True)
    print(f"⌛ Waiting for engine processing (10s)...")
    await asyncio.sleep(10)

async def main():
    os.chdir("/home/vinayb/AntiGravityProjects/knwstack/examples/smart_building")
    
    # 1. Nominal
    await run_test("telemetry", "🟢 NOMINAL")
    
    # 2. Fire Alarm
    await run_test("fire_alarm", "🔴 FIRE ALARM")
    
    # 3. High Temp
    await run_test("high_temp", "🟠 HIGH TEMP")
    
    # 4. Anomaly
    await run_test("anomaly", "🔵 ANOMALY")
    
    # 5. Campus Sim
    await run_test("campus_sim", "🏢 CAMPUS SIM")

    print("\n✅ All tests dispatched. Check engine logs for results.")

if __name__ == "__main__":
    asyncio.run(main())
