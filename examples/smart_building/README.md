# Smart Building Reference Implementation

This example demonstrates how to build a KnwStack application that processes HVAC telemetry from a smart building using the **Split-Brain Architecture**.

## 1. Setup & Activate

Ensure NATS is running in the root directory (`docker compose up -d`). Then, set up and activate your environment:

```bash
uv sync
source .venv/bin/activate
```

## 2. Run the Application

Once activated, you can run the application directly with plain Python:

```bash
cd examples/smart_building
python app.py
```

### 🚀 Hybrid Performance
This example is configured in **Hybrid Mode** by default:
- **Alarms** use the **SuperHot (Push)** path for absolute minimum latency.
- **Telemetry** uses the **Reliable (Pull)** path to ensure zero data loss for AI analysis.

The framework automatically provisions the necessary NATS JetStream infrastructure on startup.

## 3. Test the System

In a new terminal window, **activate the venv**, and use the provided `generator.py` script to simulate different real-world scenarios.

**Nominal Telemetry (Reliable Path)**
```bash
python generator.py --mode telemetry
```

**Hot Path (Immediate Reflex)**
```bash
python generator.py --mode fire_alarm
```
*Look at the engine terminal: You will see the Sub-10ms Reflex Rule trigger an immediate shutdown.*

**Warm Path (Tactical Response)**
```bash
python generator.py --mode high_temp
```
*Look at the engine terminal: The Tactical Model will calculate the 1-second rolling average and trigger a cooling increase.*

**Cold Path (Strategic Diagnosis)**
```bash
# Ensure you have OPENAI_API_KEY in your .env or environment!
python generator.py --mode anomaly
```
*Look at the engine terminal: The Strategic Prompt will detect the mismatch between power draw and temperature and ask the LLM for a diagnosis.*
