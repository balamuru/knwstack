# Mastering KnwStack: A Tutorial on Real-Time "Split-Brain" AI

Welcome to this hands-on tutorial on KnwStack! If you're building systems that need to process thousands of events per second while simultaneously running deep, complex reasoning with LLMs, you've likely encountered a massive bottleneck: LLMs are slow, and streams are fast. 

In this blog, we'll take a deep dive into how KnwStack solves this problem using its unique **"Split-Brain" Architecture**. We'll tour the codebase, explore ingestion modes, and show you how to write your own application.

---

## Module 1: The "Split-Brain" Architecture

The fundamental problem KnwStack solves is the **tension between speed and intelligence**. If you put an LLM in the middle of a high-speed data stream, you will block the stream and create fatal backpressure.

To solve this, KnwStack separates data streams into three distinct execution paths:

1. **The Reflex Path (Hot)**: Sub-10ms latency for immediate, deterministic physical actions. This path must *never* be blocked.
2. **The Tactical Path (Warm)**: Sub-100ms latency for fast local machine learning inference or windowed heuristics.
3. **The Strategic Path (Cold)**: Seconds of latency for deep Cloud LLM reasoning. LLM network calls use non-blocking background threads so they operate asynchronously.

### The Technology Stack
* **Messaging:** **NATS JetStream** provides lightweight, exactly-once delivery and subject-based routing (e.g., `tenant.app.event`).
* **Stream Engine:** **Bytewax** gives us Rust-level performance with a pure Python developer API, handling stateful windowing natively.
* **AI Orchestration:** **LiteLLM** keeps the Cold Path lightweight and allows routing prompts to 100+ LLMs seamlessly.

---

## Module 2: Codebase Tour & Project Structure

Understanding the KnwStack repository is critical for mastering the framework. Let's look at `src/knwstack/`:

* **`api/` (Developer Abstractions):** This is what you interact with most. It contains the decorators (`@reflex_rule`, `@tactical_model`, etc.) that register your logic into global registries.
* **`engine/` (The Core Router):** Found in `engine/router.py`, this is the heavy lifter. It wires your logic into a high-performance Bytewax dataflow, actively splitting the Hot, Warm, and Cold paths so they never block each other.
* **`connectors/` (I/O Operators):** Contains custom NATS JetStream connectors (e.g., `NatsSource` and `NatsSink`) that handle getting data in and out of the Bytewax engine.
* **`state/`:** Manages Complex Event Processing (CEP) windowing configurations to group data over time.

---

## Module 3: Ingestion Modes & NATS

Data enters the engine through NATS, and KnwStack supports two distinct ingestion strategies:

### 1. Reliable Mode (`NatsSource`)
Uses NATS JetStream "Pull Subscriptions". 
* **Benefit:** It's persistent. If your app crashes, NATS remembers where you left off. You can even replay historical data.
* **Use Case:** Tactical (ML) and Strategic (LLM) analysis.

### 2. SuperHot Mode (`NatsCoreSource`)
Bypasses JetStream and uses NATS Core "Push Subscriptions".
* **Benefit:** Fire-and-forget. Messages are pushed into memory in sub-milliseconds with zero disk overhead.
* **Use Case:** Strictly for the Reflex path (e.g., immediate alarms).

### Hybrid Ingestion
You can mix and match! Using the `build_engine` function, you can ingest alarms in SuperHot mode and telemetry in Reliable mode simultaneously:

```python
engine = build_engine(
    nats_url="nats://localhost:4222",
    inputs={
        "tenant.app.alarms": "superhot",  
        "tenant.app.telemetry": "reliable" 
    }
)
```

---

## Module 4: The Developer API

Building an app is incredibly simple. You don't manage threads or window boundaries; you just use decorators:

```python
from knwstack.api.decorators import reflex_rule, tactical_model, strategic_prompt

# 1. HOT PATH: Runs instantly (<10ms)
@reflex_rule("tenant.telemetry.>")
def emergency_stop(events):
    for _, data in events:
        if data.get("temp", 0) > 100:
            return {"action": "shutdown"}
    return None

# 2. WARM PATH: Runs on windowed data (<100ms)
@tactical_model("tenant.telemetry.>")
def detect_anomalies(events):
    # Run a fast local ML model over the 1-second window
    pass

# 3. COLD PATH: Sent to an LLM in the background
@strategic_prompt("tenant.telemetry.>", cooldown_s=60)
def diagnose_issue(events):
    # Construct a prompt for LiteLLM
    return {
        "model": "gpt-4",
        "messages": [{"role": "user", "content": f"Analyze these {len(events)} events..."}]
    }
```

---

## Module 5: Critical Framework "Gotchas"

If you are modifying the core framework, keep these in mind:

> [!WARNING]
> **Never Merge the Hot and Windowed Streams:** Bytewax synchronizes epochs on merged streams. If you merge the Reflex actions with the Warm/Cold window boundaries, the instant Reflex actions will be delayed!

> [!IMPORTANT]
> **Async Background Threads:** Because Bytewax is a synchronous engine, `nats-py` async clients and `litellm` async calls MUST run in dedicated background Python threads. Otherwise, the Bytewax worker will block, and NATS connections will drop.

---

## Module 6: Hands-On with the Smart Building Example

To see everything come together, let's examine the `examples/smart_building/app.py` application. This example perfectly illustrates the hybrid ingestion and split-brain execution logic.

### 1. Hybrid Ingestion Setup
At the bottom of the script, the Bytewax engine is initialized:
```python
flow = build_engine(
    nats_url="nats://localhost:4222", 
    inputs={
        "bldg1.hvac.alarm": "superhot",     # Zero-latency push for alarms
        "bldg1.hvac.telemetry": "reliable"  # Guaranteed JetStream pull for analytics
    },
    output_subject="bldg1.actions"
)
```
Notice how it ingests fire alarms using `superhot` mode to bypass disk persistence, while regular telemetry uses `reliable` mode.

### 2. The Hot Path: Reflex Action
If a fire alarm is triggered, it must be handled instantly:
```python
@reflex_rule("bldg1.hvac.alarm")
def fire_alarm_reflex(events):
    for topic, data in events:
        if topic == "bldg1.hvac.alarm" and data.get("type") == "fire":
            return {"action": "shutdown", "reason": "fire_alarm", "building": "bldg1"}
    return None
```
This runs in sub-10ms because it is never grouped into a CEP window.

### 3. The Warm Path: Tactical Analytics
For telemetry, we group events over a 1-second window to calculate the rolling average temperature:
```python
@tactical_model("bldg1.hvac.telemetry")
def temperature_tactical(events):
    # Calculates rolling averages and increases cooling if temp > 28.0C
```

### 4. The Cold Path: Strategic LLM Diagnosis
Finally, we check for complex mechanical anomalies (e.g., high power draw but low temperature). If detected, we send it to an LLM:
```python
@strategic_prompt("bldg1.hvac.telemetry", cooldown_s=60)
def anomaly_strategic(events):
    # Logic to detect anomaly...
    return {
        "model": "gpt-4o-mini",
        "messages": [{"role": "user", "content": "The HVAC system is drawing over 10kW..."}]
    }
```
The framework automatically runs this OpenAI call in a background Python thread, ensuring the `fire_alarm_reflex` is never blocked!

## Next Steps

Ready to run it yourself? 
Use the CLI scripts in the repository to spin up a local NATS instance and start injecting mock telemetry. Watch how the Hot path fires instantly while the Cold path quietly thinks in the background!

Happy streaming!
