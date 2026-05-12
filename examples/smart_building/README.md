# Smart Building Reference Implementation

This example demonstrates how to build a KnwStack application that processes HVAC telemetry from a smart building. 

It highlights the **Split-Brain Architecture** by implementing:
1. **Hot Path (Reflex)**: Instant HVAC shutdown upon detecting a fire alarm.
2. **Warm Path (Tactical)**: 1-second rolling averages of temperature to adjust cooling.
3. **Cold Path (Strategic)**: Asynchronous LLM reasoning to diagnose complex anomalies (e.g., high power draw despite low temperature).

## Prerequisites

1. Ensure the NATS infrastructure is running from the root of the project:
   ```bash
   docker compose up -d
   ```

2. **Python Environment**: KnwStack uses a virtual environment managed by `uv`. You have two options for running commands:
   *   **Using `uv run` (Recommended)**: Prefix commands with `uv run` (e.g., `uv run python ...`). This handles the environment automatically and ensures your dependencies are in sync.
   *   **Manual Activation**: Activate the environment manually if you prefer not to type the prefix:
       ```bash
       source .venv/bin/activate
       ```

## Step 1: Run the Application

The KnwStack framework operates over the Pathway stream processing engine. You run your application by simply executing the Python script:

```bash
uv run python app.py
```

### 🚀 Hybrid Performance
This example is configured in **Hybrid Mode** by default:
- **Alarms** use the **SuperHot (Push)** path for absolute minimum latency.
- **Telemetry** uses the **Reliable (Pull)** path to ensure zero data loss for AI analysis.

The framework automatically provisions the necessary NATS JetStream infrastructure on startup.

## Step 2: Inject Telemetry (Test the System)

In a new terminal window, use the provided `generator.py` script to simulate different real-world scenarios. 

**Nominal Telemetry (Reliable Path)**
```bash
uv run python generator.py --mode telemetry
```

**Hot Path (Immediate Reflex)**
```bash
uv run python generator.py --mode fire_alarm
```
*Look at the engine terminal: You will see the Sub-10ms Reflex Rule trigger an immediate shutdown.*

**Warm Path (Tactical Response)**
```bash
uv run python generator.py --mode high_temp
```
*Look at the engine terminal: The Tactical Model will calculate the 1-second rolling average and trigger a cooling increase.*

**Cold Path (Strategic Diagnosis)**
```bash
# Ensure you have OPENAI_API_KEY in your .env or environment!
uv run python generator.py --mode anomaly
```
*Look at the engine terminal: The Strategic Prompt will detect the mismatch between power draw and temperature, gather the context window, and asynchronously ask the LLM for a diagnosis.*
