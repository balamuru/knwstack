# The KnwStack Handbook

Welcome to the definitive guide for KnwStack, the Real-Time AI Framework designed for high-velocity streaming and deep reasoning.

---

## 1. The "Split-Brain" Architecture

KnwStack solves the fundamental **tension between speed and intelligence**. Traditional streaming systems are fast but "dumb" (deterministic), while AI agent frameworks are "smart" but slow (request-response).

KnwStack implements a **"Split-Brain"** paradigm, separating data processing into three distinct execution paths that run in parallel:

1.  **The Reflex Path (Hot)**: Sub-10ms latency for immediate, deterministic physical actions.
2.  **The Tactical Path (Warm)**: Sub-100ms latency for fast local machine learning inference or windowed heuristics.
3.  **The Strategic Path (Cold)**: Seconds of latency for deep Cloud LLM reasoning via asynchronous background tasks.

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

## 3. Developer API Reference

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
