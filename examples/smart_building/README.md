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

## Step 1: Create the NATS Stream

Before running the application, we need to define a JetStream for our `bldg1.>` subjects. We can use the `nats-box` Docker container to do this:

```bash
docker compose exec nats-box nats stream add BLDG1_STREAM \
    --subjects "bldg1.>" \
    --storage file \
    --retention limits \
    --discard old \
    --max-msgs=-1 \
    --max-bytes=-1 \
    --max-age=1h \
    --dupe-window=2m \
    --defaults
```

## Step 2: Run the Application

The KnwStack framework operates over the Bytewax stream processing engine. You run your application by passing your Python file and the compiled `flow` object to the Bytewax runner.

Run this from the `examples/smart_building` directory:

```bash
cd examples/smart_building/
uv run python -m bytewax.run app:flow
```

*Note: The engine will start up and block, waiting for events on the `bldg1.>` subjects.*

## Step 3: Inject Telemetry (Test the System)

In a new terminal window, use the provided `generator.py` script to simulate different real-world scenarios. 

**Normal Telemetry (No Actions Triggered)**
```bash
uv run python generator.py --mode telemetry_normal
```

**Hot Path (Fire Alarm)**
```bash
uv run python generator.py --mode fire
```
*Look at the engine terminal: You will see the Sub-10ms Reflex Rule trigger an immediate shutdown.*

**Warm Path (Temperature Spike)**
```bash
uv run python generator.py --mode telemetry_hot
```
*Look at the engine terminal: The Tactical Model will calculate the 1-second rolling average and trigger a cooling increase.*

**Cold Path (Complex Anomaly)**
```bash
# Ensure you have OPENAI_API_KEY exported in your environment!
export OPENAI_API_KEY="your-key-here"
uv run python generator.py --mode anomaly
```
*Look at the engine terminal: The Strategic Prompt will detect the mismatch between power draw and temperature, gather the context window, and asynchronously ask the LLM for a diagnosis without blocking the engine.*
