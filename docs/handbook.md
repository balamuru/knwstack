# KnwStack Handbook
*Where Knowledge meets Now.*

---

## 1. The Split-Brain Architecture

KnwStack implements a unique "Split-Brain" design to solve the AI Latency Paradox: How do you react in milliseconds to a physical event while reasoning for seconds about its cause?

The framework decouples your logic into three distinct paths:
1.  **The Reflex Path (Hot)**: Sub-10ms deterministic execution for safety-critical actions.
2.  **The Tactical Path (Warm)**: Sub-100ms latency for fast local machine learning inference or windowed heuristics.
3.  **The Strategic Path (Cold)**: Seconds of latency for deep Cloud LLM reasoning via asynchronous background tasks.

### Mapping Events to Paths
In a real-world scenario, you typically have 4 levels of interaction:
1.  **Ingestion (Green)**: The baseline "hum" of telemetry (e.g., normal temp readings).
2.  **Reflex (Red)**: The "Hot" response to a binary trigger (e.g., Fire Alarm).
3.  **Tactical (Orange)**: The "Warm" response to a windowed trend (e.g., High Temp over 5s).
4.  **Strategic (Blue)**: The "Cold" response to an unknown anomaly requiring LLM reasoning.

---

## 2. Why this Stack? (Design Decisions)

*   **NATS JetStream**: Provides a sub-millisecond messaging backbone with persistent "Time Travel" capabilities for windowed replay.
*   **Pathway**: A unified streaming engine that allows us to write Python that compiles into an incremental dataflow.
*   **LiteLLM**: A universal interface to 100+ LLMs, enabling the Strategic path to switch models without code changes.

---

## 3. KnwStack vs. The World (Alternatives)

KnwStack is not a replacement for general-purpose message brokers; it is a **Specialized AI Router**.

| Feature | KnwStack | Confluent (Kafka) | Redpanda | LangChain |
| :--- | :--- | :--- | :--- | :--- |
| **Primary Goal** | **AI Routing** | Data Persistence | Fast Ingestion | Agent Logic |
| **Split-Brain** | **Native (Hot/Cold)** | ❌ (Manual Setup) | ❌ (Manual Setup) | ❌ (Synchronous) |
| **Latency Focus** | **Sub-10ms** | 100ms+ | 10ms - 50ms | Seconds (API) |
| **Infrastructure** | Single Binary (NATS) | Complex (JVM/Zoo) | Single Binary | Library |
| **Programming** | Python Decorators | Java/Scala Heavy | Kafka API | Chains/Graphs |

---

## 4. Positioning: When to use KnwStack

### 4.1 Ideal Use Cases (The "Who/What/Where")
*   **Edge Intelligence**: React to a sensor over-temp in <10ms while asking an LLM for a post-mortem analysis.
*   **High-Velocity Monitoring**: Smart Buildings, Industrial IoT, and FinTech signals.
*   **Autonomous Synthesis**: Correlating fast metrics with slow reasoning plans.

### 4.2 What KnwStack is NOT meant for
*   **Data Lake Storage**: Use Kafka or S3 for multi-year archival.
*   **Batch Processing**: Use Spark or Polars for 3-year historical averages.
*   **Simple CRUD**: Use FastAPI or Django for standard web profiles.

---

## 5. Developer API Reference

Building a KnwStack application is handled entirely through simple Python decorators.

### 5.1 @reflex_rule
For the **Hot Path**. Must be deterministic and execute in <10ms.
```python
@reflex_rule("bldg1.hvac.>") # Hierarchical wildcard
def emergency_stop(events):
    for topic, data in events:
        if data.get("type") == "fire":
            return {"action": "shutdown"}
```

### 5.2 @tactical_model
For the **Warm Path**. The engine for **CEP and Windowing**.
```python
@tactical_model(">.telemetry", window_type="sliding", length_s=5)
def detect_trend(events):
    # 'events' is grouped by 'key' automatically
    pass
```

### 5.3 @strategic_prompt
For the **Cold Path**. Dispatches asynchronous LLM requests.
```python
@strategic_prompt(">.telemetry", cooldown_s=60)
def analyze_cause(events):
    return {"model": "gpt-4o-mini", "messages": [...]}
```

---

## 6. Observability & Operational Modes

KnwStack supports three distinct operational modes to balance development visibility with production performance.

### 6.1 Headless Mode (Production)
By default, the `KnwStackRunner` executes in **Headless Mode**. This is a silent, high-performance mode suitable for background services and containerized deployments. No web server or terminal UI is launched.

### 6.2 Dashboard Mode (Web Observability)
When launched with `--dashboard`, KnwStack activates a Prometheus-compatible metrics server at `http://localhost:9090/metrics`. In this mode, the terminal remains clean and silent (thanks to our internal silence patch).

### 6.3 Stats Mode (Terminal Observability)
When launched with `--stats`, KnwStack enables the classic Pathway terminal UI. This provides real-time flickering statistics directly in your console. No web server is launched in this mode.

### 6.4 Stress Testing & Benchmarking
KnwStack includes a dedicated stress testing mode in the generator to verify framework performance. It slams the engine with concurrent nominal telemetry while bypassing LLM calls.

*   **Command**: `python generator.py --stress --workers 100`
*   **Metric Polling**: The generator will automatically poll the engine's `/metrics` endpoint to display real-time throughput and internal latency.

> [!TIP]
> On a standard developer machine, KnwStack typically sustains **~1,800 msg/s** across 100 concurrent tenant streams.

### 6.5 Log Narrative
Even in silent modes, KnwStack provides a transparent "narrative" of every event:
    *   `⚡ [INGEST]`: A raw event enters the system.
    *   `   ∟ [HOT]`: Reflex path evaluation.
    *   `🟠 [WARM]`: Tactical path windowed evaluation (CEP).
    *   `🔵 [COLD]`: Strategic path LLM dispatch.

### 6.5 Tuning Verbosity
Run your app with `--log DEBUG` to see the full dataflow state, or `INFO` for the high-level narrative.

---

## 7. Advanced Patterns: Windowing & CEP

KnwStack leverages Pathway's incremental engine to perform **Complex Event Processing (CEP)** with minimal code.

### 7.1 Complex Event Processing (CEP)
CEP is the core of the Tactical path. It allows you to correlate multiple events into a single "high-level" event.
*   **Scenario**: Detect a potential fire before the alarm pulls.
*   **CEP Rule**: "If Temp > 80 AND Smoke > 20% over a 5s window, trigger warning."

### 7.2 Smart Building Implementation (CEP in Action)
Explore `examples/smart_building/` to see **CEP in action**:
1.  **Start Engine**: `python app.py --log INFO`
2.  **CEP Demo**: Trigger "High Temp" in the generator. Notice how the Tactical path waits for a trend in the telemetry window before firing.

### 7.3 Anatomy of a CEP Log
When running the Smart Building example, you will see a "narrative" of the engine thinking:

```text
INFO     🟠 [WARM] Evaluating Tactical Model 'temperature_tactical' for bldg1.hvac.telemetry (Window: 5 events)
WARNING  ⚠️ High average temperature detected (30.1°C). Increasing cooling.
WARNING     ∟ [WARM] Outcome: ACTION TRIGGERED -> {'action': 'set_cooling', 'value': 'high', ...}
```

**What are you looking for?**
1.  **`Evaluating... (Window: 5 events)`**: The engine has successfully collected enough events to satisfy your window requirement (e.g., 5 seconds of telemetry).
2.  **`⚠️ High average...`**: This is your **custom business logic** executing. It only fires when the condition (Average > 30°C) is met across the *entire* window.
3.  **`∟ [WARM] Outcome: ACTION TRIGGERED`**: The KnwStack router has validated your rule's return value and is now dispatching the action (e.g., to NATS or a physical actuator).
4.  **`🔵 [COLD] Evaluating...`**: Note that this often appears alongside the Warm path. This is the **Split-Brain** in action—one side is handling the reflex/tactical response, while the other is preparing a deep-reasoning prompt.

### 7.4 Temporal Synchronization (Heartbeats)
In high-throughput systems, the windowing clock moves naturally. However, in sparse or intermittent streams (like building sensors), windows can "stall" because no new events are arriving to push the watermark forward. 

**Best Practice**: Always inject a periodic `heartbeat` event into your stream (e.g., every 1s). The KnwStack engine uses these heartbeats to advance the global clock and ensure that tactical windows close reliably even when sensors are quiet.

---

## 8. Publishing to PyPI

KnwStack is configured for seamless distribution as a Python library.

### 8.1 Building Locally
Before publishing, you can verify your package build locally:
```bash
# Creates .whl and .tar.gz in the dist/ folder
uv build
```

### 8.2 Trusted Publishing (OIDC)
The provided GitHub Action uses **Trusted Publishing**, which is the most secure way to publish. It eliminates the need for manual API tokens or passwords.

**To set this up:**
1.  **Register**: Create a free account at [pypi.org](https://pypi.org).
2.  **Add Publisher**: Go to your [PyPI Settings](https://pypi.org/manage/account/publishing/) and add a new **Pending Publisher**:
    *   **PyPI Project Name**: `knwstack`
    *   **Owner**: `balamuru`
    *   **Repository**: `knwstack`
    *   **Workflow Name**: `publish.yml`

### 8.3 The Release Workflow
Once Trusted Publishing is configured, you never have to publish manually again. The loop is:
1.  **Bump Version**: Update `version = "x.y.z"` in `pyproject.toml`.
2.  **Tag**: Create a git tag: `git tag v0.1.0`.
3.  **Push**: `git push --tags`.
4.  **Release**: Go to your GitHub Repository -> **Create a new release** for that tag.

The [publish.yml](file:///home/vinayb/AntiGravityProjects/knwstack/.github/workflows/publish.yml) workflow will automatically build and upload the library to PyPI. Your users can then simply run `pip install knwstack`.

---

## 9. Architecture at Scale (Multi-Pod Deployment)

KnwStack is designed to scale horizontally across multiple pods or containers using NATS JetStream and partitioned dataflows.

### 9.1 Competing Consumers
When deploying multiple instances of a KnwStack app, they join a shared **Durable Consumer Group** in NATS.
*   **Load Balancing**: NATS automatically distributes incoming events across the available pods.
*   **Reliability**: If one pod fails, NATS redistributes its unacknowledged messages to healthy pods.

### 9.2 State Correlation & Partitioning
For **Tactical (Warm)** and **Strategic (Cold)** paths that rely on windowed state, KnwStack uses **Key-Based Partitioning**:
*   **The Key**: Events are partitioned by a specific field (e.g., `building_id` or `device_id`).
*   **State Integrity**: NATS ensures that all events with the same key are routed to the same pod. This allows the local Pathway engine to maintain a consistent sliding window for that specific entity without needing a global distributed database.

### 9.3 Scaling Strategy
*   **Stateless Scaling**: The **Hot Path** (Reflexes) scales linearly with the number of pods.
*   **Stateful Scaling**: The **Warm/Cold Paths** scale by increasing the number of partitions in the NATS Stream, allowing more pods to share the stateful workload.

---

## 10. Multi-Tenant Operations

KnwStack is built from the ground up for massive multi-tenancy. Whether you are managing 5 buildings or 5,000, the framework ensures strict isolation.

### 10.1 Key-Based Partitioning
Every event ingested by KnwStack is assigned a `key`. This key is the "Owner ID" of the event (e.g., a Building ID, Account ID, or Device ID). 

*   **Explicit Keys**: If your JSON payload includes a `"key"` field, KnwStack uses it.
*   **Implicit Keys**: If the payload is missing a key, KnwStack automatically falls back to the first segment of the NATS subject (e.g., in `bldg1.hvac.telemetry`, the key is `bldg1`).

### 10.2 Independent Windows
By grouping dataflows by `key`, KnwStack ensures that:
-   **No Cross-Talk**: Building A's high-temp events never contribute to Building B's tactical averages.
-   **Isolation**: Building B can be under an "Anomaly" analysis without affecting the latency or processing state of Building A.
-   **Resource Efficiency**: The engine only maintains window state for *active* tenants.
