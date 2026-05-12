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
* **Stream Engine:** **Pathway** gives us unified Rust-level performance with a pure Python developer API, handling stateful windowing natively with incremental computation.
* **AI Orchestration:** **LiteLLM** keeps the Cold Path lightweight and allows routing prompts to 100+ LLMs seamlessly.

---

## Module 2: Parallel Brains (One Event, Multiple Reactions)

In a traditional "Step-Function" or "Chain" architecture, you have to choose: do I act now, or do I think first? 

KnwStack allows for **Parallel Fan-Out**. A single event entering the engine is instantly broadcast to all three paths. This means you don't have to sacrifice intelligence for speed. 

Consider a **Fire Alarm** event:
1. **The Reflex (Hot)**: Instantly cuts power to the HVAC to prevent smoke spread (<10ms).
2. **The Strategic (Cold)**: Simultaneously dispatches the alarm data to an LLM to generate a safety report for emergency responders (Seconds).

The system "survives" first via the Reflex path, and "learns" or "plans" second via the Strategic path—all triggered by the same initial event.

---

## Module 3: Why this Stack? (Design Decisions)

You might wonder why we didn't use the "industry standard" big data stack (Kafka + Flink + LangChain). Here is the rationale:

### 1. NATS JetStream vs. Apache Kafka
Kafka is powerful, but it’s heavy. It requires Zookeeper/KRaft and significant JVM memory. **NATS** is a single, lightweight binary that provides the same at-least-once delivery and replayability we need, but with sub-millisecond subject-based routing. For Edge AI and responsive streams, NATS is simply faster and easier to operate.

### 2. Pathway vs. Apache Flink
We wanted a **Pure Python API** for AI engineers, but we needed **Rust-level performance** for the Hot Path (<10ms). Apache Flink is Java-heavy and making it play nice with Python (PyFlink) often introduces significant latency. **Pathway** gives us a Rust-backed core with an incremental computation model, allowing us to write logic in Python while maintaining the speed of a compiled language.

### 3. LiteLLM vs. Heavy Frameworks
In a streaming engine, overhead is the enemy. While frameworks like LangChain are great for building agents, they can be "heavy" for a high-throughput router. **LiteLLM** provides a lean, OpenAI-compatible interface to 100+ LLM providers without adding unnecessary complexity to our async background tasks.

---

## Module 4: Codebase Tour & Project Structure

Understanding the KnwStack repository is critical for mastering the framework. Let's look at `src/knwstack/`:

* **`api/` (Developer Abstractions):** This is what you interact with most. It contains the decorators (`@reflex_rule`, `@tactical_model`, etc.) that register your logic into global registries.
* **`engine/` (The Core Router):** Found in `engine/router.py`, this is the heavy lifter. It wires your logic into a high-performance Pathway dataflow, actively splitting the Hot, Warm, and Cold paths so they never block each other.

---

## Module 5: The Developer API

Building an app is incredibly simple. You don't manage threads or window boundaries; you just use decorators. KnwStack automatically tags events with their NATS subjects, so your rules always know where data came from.

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
@tactical_model("tenant.telemetry.>", window_type="sliding", length_s=5, slide_s=1)
def detect_anomalies(events):
    # Run a fast local ML model over the 5-second window
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

## Module 6: Initializing the Engine

KnwStack uses the `build_engine` factory to wire everything together. It handles the NATS connectivity and Pathway graph construction for you.

```python
from knwstack.engine.router import build_engine

engine = build_engine(
    nats_url="nats://localhost:4222",
    inputs=["tenant.telemetry.alarm", "tenant.telemetry.metrics"],
    output_subject="tenant.actions",
    jetstream_stream="KNWSTACK_STREAM"
)

if __name__ == "__main__":
    engine.run()
```

---

## Module 7: Critical Framework Concepts

### Determinism & State
Pathway is an incremental engine. This means your rules and models should be deterministic. For time-based operations, KnwStack expects a `timestamp` field in your JSON payloads. This ensures that even if events are delayed, the windowing results remain consistent.

### Async Background Tasks
While Pathway handles the dataflow, KnwStack uses `litellm` in the Strategic path to call LLMs asynchronously. This ensures that a slow LLM response never blocks the immediate Reflex path.

---

## Module 8: Hands-On with the Smart Building Example

To see everything come together, let's examine the `examples/smart_building/app.py` application. 

### 1. The Hot Path: Reflex Action
If a fire alarm is triggered, it must be handled instantly:
```python
@reflex_rule("bldg1.hvac.alarm")
def fire_alarm_reflex(events):
    for topic, data in events:
        if topic == "bldg1.hvac.alarm" and data.get("type") == "fire":
            return {"action": "shutdown", "reason": "fire_alarm", "building": "bldg1"}
    return None
```

### 2. The Warm Path: Tactical Analytics
For telemetry, we group events over a sliding window to calculate rolling averages:
```python
@tactical_model("bldg1.hvac.telemetry", window_type="sliding", length_s=5, slide_s=1)
def temperature_tactical(events):
    # Calculates rolling averages and increases cooling if temp > 28.0C
```

### 3. The Cold Path: Strategic LLM Diagnosis
Finally, we check for complex mechanical anomalies or perform post-mortem analysis on emergencies. For example, we can trigger an AI safety report whenever a fire alarm occurs:
```python
@strategic_prompt("bldg1.hvac.alarm", cooldown_s=5)
def fire_analysis_strategic(events):
    # Logic to generate a safety assessment for the maintenance crew
    return {
        "model": "gpt-4o-mini",
        "messages": [...]
    }
```

---

## Module 9: Observing the Engine's "Narrative"

One of the most powerful features of KnwStack is its transparent logging. Because the engine is complex, we’ve implemented a descriptive narrative that lets you follow an event through the "Split-Brain."

When you run the engine, you'll see logs like this:
* `⚡ [INGEST]`: A raw event enters the system.
* `   ∟ [HOT]`: The Reflex path checking for immediate deterministic rules.
* `🟠 [WARM]`: The Tactical path evaluating windowed metrics.
* `🔵 [COLD]`: The Strategic path dispatching asynchronous AI prompts.

This observability makes it trivial to debug why a specific AI prompt was (or wasn't) triggered based on the incoming stream.

### Tuning Verbosity for Performance
In a production environment with thousands of events, you don't want to log every single ingestion. KnwStack uses standard Python `logging` levels to help you tune performance:

* **DEBUG**: Shows everything, including `⚡ [INGEST]` for every event and `∟ No action taken` for every path. Best for development.
* **INFO (Default)**: Shows rule matching and the start of path evaluations. Best for staging/monitoring.
* **WARNING**: Only shows actual actions being triggered and LLM dispatches. Best for high-throughput production.

You can control this via the `--log` flag when starting your application:

```bash
python app.py --log DEBUG
```

By default, the system runs at **INFO** level, providing a balanced narrative without overwhelming your terminal.

---

## Next Steps: Run it Yourself

Ready to see the Split-Brain in action? We’ve made the setup opinionated and simple:

1. **Spin up NATS**: `docker compose up -d`
2. **Setup Environment**:
   ```bash
   uv sync
   source .venv/bin/activate
   ```
3. **Run the Example**:
   ```bash
   cd examples/smart_building
   python app.py
   ```
4. **Trigger Events**: In a separate terminal, use the **Interactive Generator**:
   ```bash
   python generator.py
   ```

Select an option in the generator menu (like **Option 2: Fire Alarm**) and watch the engine logs instantly snap into action!

Happy streaming!
