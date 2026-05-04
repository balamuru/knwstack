#!/usr/bin/env python3
"""
<name>KnwStack Test Event Injector</name>
<description>
A quick utility to publish mock JSON telemetry directly into NATS JetStream. 
Useful for simulating real-time events to trigger the Hot/Warm/Cold dataflow paths 
during local development.
</description>
<trigger>When the user asks to test the knwstack engine, simulate an event, or publish test telemetry.</trigger>
"""
import argparse
import asyncio
import json
import nats

async def inject_event(nats_url: str, subject: str, payload_dict: dict):
    try:
        nc = await nats.connect(nats_url)
        payload_bytes = json.dumps(payload_dict).encode()
        await nc.publish(subject, payload_bytes)
        print(f"🚀 Successfully injected event to '{subject}':")
        print(json.dumps(payload_dict, indent=2))
        await nc.close()
    except Exception as e:
        print(f"❌ Failed to connect or publish to NATS: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inject a test event into NATS for KnwStack.")
    parser.add_argument("--url", default="nats://localhost:4222", help="NATS connection URL")
    parser.add_argument("--subject", required=True, help="Target NATS subject (e.g. weather.temp)")
    parser.add_argument("--payload", required=True, help="JSON payload string (e.g. '{\"temp\": 105}')")
    
    args = parser.parse_args()
    
    try:
        data = json.loads(args.payload)
    except json.JSONDecodeError:
        print("❌ Invalid JSON payload.")
        exit(1)
        
    asyncio.run(inject_event(args.url, args.subject, data))
