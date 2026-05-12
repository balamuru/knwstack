# The KnwStack Handbook

Welcome to the definitive guide for KnwStack, the Real-Time AI Framework designed for high-velocity streaming and deep reasoning.

---

## 1. The "Split-Brain" Architecture

KnwStack solves the fundamental **tension between speed and intelligence**. Traditional streaming systems are fast but "dumb" (deterministic), while AI agent frameworks are "smart" but slow (request-response).

KnwStack implements a **"Split-Brain"** paradigm, separating data processing into three distinct execution paths that run in parallel:

1.  **The Reflex Path (Hot)**: Sub-10ms latency for immediate, deterministic physical actions.
2.  **The Tactical Path (Warm)**: Sub-100ms latency for fast local machine learning inference or windowed heuristics.
3.  **The Strategic Path (Cold)**: Seconds of latency for deep Cloud LLM reasoning via asynchronous background tasks.

### Mapping Events to Paths
In a real-world scenario, you typically have 4 levels of interaction:
1.  **Ingestion (Green)**: The baseline "hum" of telemetry (e.g., normal temp readings).
2.  **Reflex (Red)**: The "Hot" response to a binary trigger (e.g., Fire Alarm).
3.  **Tactical (Orange)**: The "Warm" response to a windowed trend (e.g., High Temp over 5s).
4.  **Strategic (Blue)**: The "Cold" response to an unknown anomaly requiring LLM reasoning.

### Parallel Execution (Fan-Out)
Unlike traditional "chains," KnwStack broadcasts a single event to all three paths simultaneously. This allows the system to **survive first** (Reflex) and **reason second** (Strategic) without any blocking.

---

## 2. Why this Stack? (Design Decisions)

KnwStack is built on a high-performance **Python + Rust Edge Stack**. We chose these technologies to provide a perfect balance between developer experience and production-grade speed.

### 2.1 Messaging: NATS JetStream over Apache Kafka
*   **Decision:** We exclusively use NATS JetStream for the messaging backbone.
*   **Rationale:** Kafka is powerful but carries immense operational overhead (JVM bloat, Zookeeper/KRaft complexity). NATS is a single, lightweight Go binary that provides exactly-once delivery, replayability, and sub-millisecond subject-based routing. It is built for the Edge and for responsiveness.

### 2.2 Stream Engine: Pathway over Apache Flink
*   **Decision:** Core routing and stateful windowing are handled by Pathway.
*   **Rationale:** We wanted a pure Python API for AI engineers but required Rust-level performance for the Hot Path (<10ms). Apache Flink is Java-heavy and PyFlink often introduces significant latency. Pathway provides a Rust-backed core with an incremental computation model, allowing us to write logic in Python while maintaining the speed of a compiled language.

### 2.3 AI Orchestration: LiteLLM
*   **Decision:** Strategic prompts are dispatched via LiteLLM.
*   **Rationale:** In a streaming engine, overhead is the enemy. LiteLLM provides a lean, OpenAI-compatible interface to 100+ LLM providers without the complexity of heavy orchestration frameworks, keeping the asynchronous Cold Path efficient and provider-agnostic.

---

## 3. KnwStack vs. The World (Alternatives)

KnwStack is not a replacement for general-purpose message brokers; it is a **Specialized AI Router**.

| Feature | KnwStack | Apache Kafka / Confluent |
| :--- | :--- | :--- |
| **Primary Goal** | Latency-Aware AI Routing | Durable Data Ingestion & Persistence |
| **Philosophy** | Split-Brain (Hot/Warm/Cold) | Unified Log Append |
| **Dev Experience** | Pure Python Decorators | Java/Scala Heavy |
| **Infrastructure** | Single Binary (NATS) | Complex (Zookeeper/KRaft/JVM) |
| **Best For** | Real-time AI Reflexes | Multi-Petabyte Pipelines |
| **Latency Focus** | Sub-10ms End-to-End | 50ms - 500ms Typical |

---

## 4. Positioning: When to use KnwStack

### 4.1 Ideal Use Cases (The "Who/What/Where")
*   **Edge Intelligence**: Where you need to react to a physical event (e.g., a sensor over-temp) in <10ms while simultaneously asking an LLM for a post-mortem analysis.
*   **High-Velocity Monitoring**: Smart Buildings, Industrial IoT, and High-Frequency Trading signals where "surviving the burst" is critical.
*   **Autonomous Synthesis**: Systems that need to correlate fast heuristic metrics with slow, high-reasoning AI plans.

### 4.2 What KnwStack is NOT meant for
*   **Data Lake Storage**: KnwStack is about **flow, not storage**. If you need to store petabytes of historical logs for 7 years, use Kafka or S3.
*   **Batch Processing**: If your task is "calculate the average temperature over the last 3 years for 1 million sensors," use Spark, Snowflake, or Polars.
*   **Simple Web APIs**: For a standard "User Profile" CRUD app, KnwStack is overkill. Use FastAPI or Django.

---

## 5. Developer API Reference

Building a KnwStack application is handled entirely through simple Python decorators. The framework manages the complex threading, windowing, and NATS connectivity for you.

### 3.1 @reflex_rule
For the **Hot Path**. Must be deterministic and execute in <10ms.
```python
@reflex_rule("tenant.telemetry.>")
def emergency_stop(events):
    for _, data in events:
        if data.get("temp") > 100:
            return {"action": "shutdown"}
```

### 3.2 @tactical_model
For the **Warm Path**. Used for windowed metrics and fast local ML inference.
```python
@tactical_model("telemetry.>", window_type="sliding", length_s=5, slide_s=1)
def detect_anomalies(events):
    # Process rolling window of events
    pass
```

### 3.3 @strategic_prompt
For the **Cold Path**. Constructs a prompt for an LLM; the engine handles the async dispatch.
```python
@strategic_prompt("telemetry.>", cooldown_s=60)
def diagnose_issue(events):
    return {
        "model": "gpt-4",
        "messages": [{"role": "user", "content": "Analyze these events..."}]
    }
```

---

## 4. Observability & Performance Tuning

KnwStack provides a transparent "narrative" of every event through the engine logs.

### 4.1 Log Narrative
*   `⚡ [INGEST]`: A raw event enters the system.
*   `   ∟ [HOT]`: Reflex path evaluation.
*   `🟠 [WARM]`: Tactical path windowed evaluation.
*   `🔵 [COLD]`: Strategic path LLM dispatch.

### 4.2 Tuning Verbosity
Use the `--log` flag to balance performance and visibility:
*   **DEBUG**: Shows every ingestion and path outcome (Best for development).
*   **INFO (Default)**: Shows rule matches and evaluation starts.
*   **WARNING**: Only shows actual action triggers and LLM dispatches (Best for production).

---

## 5. Testing & Quality Assurance

KnwStack includes a comprehensive test suite to verify your "Split-Brain" logic before deployment.

### 5.1 Running Tests
```bash
# Run the full suite
uv run pytest

# Target specific components
uv run pytest tests/test_api.py    # Decorators
uv run pytest tests/test_logic.py  # Path Logic
```

### 5.2 Test Isolation
The framework includes a `conftest.py` fixture that automatically clears the global rule registry between every test, ensuring your test cases remain isolated and deterministic.

---

## 6. Smart Building Reference Implementation

To see these concepts in action, explore `examples/smart_building/`:
1.  **Start Engine**: `python app.py --log INFO`
2.  **Trigger Events**: `python generator.py`

This example demonstrates **Hybrid Ingestion**: using SuperHot (Push) subjects for Alarms and Reliable (Pull) subjects for Telemetry, ensuring both speed and integrity.

---

## 7. Publishing to PyPI

KnwStack is configured to be published as a library via PyPI.

### 7.1 Automated Publishing
A GitHub Action is provided in `.github/workflows/publish.yml` that triggers whenever a new GitHub Release is published.

### 7.2 One-Time Setup (Trusted Publishing)
To enable the automated workflow:
1.  **Register**: Create a free account at [pypi.org](https://pypi.org).
2.  **Add Publisher**: In PyPI Settings, add a new "Pending Publisher":
    *   **PyPI Project Name**: `knwstack`
    *   **Owner**: `balamuru`
    *   **Repository**: `knwstack`
    *   **Workflow Name**: `publish.yml`
3.  **Release**: Create a new release on GitHub with a version tag (e.g., `v0.1.0`).

### 7.3 Manual Publishing
If you prefer to publish manually from your local machine:
```bash
uv build
uv publish
```
