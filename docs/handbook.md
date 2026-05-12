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
@reflex_rule("telemetry.>")
def emergency_stop(event):
    if event["type"] == "alarm":
        return {"action": "shutdown"}
```

### 5.2 @tactical_model
For the **Warm Path**. The engine for **CEP and Windowing**.
```python
@tactical_model("telemetry.>", window_type="sliding", length_s=5, slide_s=1)
def detect_trend(events):
    # 'events' is a windowed list of data points
    pass
```

### 5.3 @strategic_prompt
For the **Cold Path**. Dispatches asynchronous LLM requests.
```python
@strategic_prompt("telemetry.>", cooldown_s=60)
def analyze_cause(events):
    return {"model": "gpt-4", "messages": [...]}
```

---

## 6. Observability & Performance Tuning

### 6.1 Log Narrative
KnwStack provides a transparent "narrative" of every event:
*   `⚡ [INGEST]`: A raw event enters the system.
*   `   ∟ [HOT]`: Reflex path evaluation.
*   `🟠 [WARM]`: Tactical path windowed evaluation (CEP).
*   `🔵 [COLD]`: Strategic path LLM dispatch.

### 6.2 Tuning Verbosity
Run your app with `--log DEBUG` to see the full dataflow state, or `INFO` for the high-level narrative.

---

## 7. Advanced Patterns: Windowing & CEP

### 7.1 Complex Event Processing (CEP)
CEP is the core of the Tactical path. It allows you to correlate multiple events into a single "high-level" event.
*   **Scenario**: Detect a potential fire before the alarm pulls.
*   **CEP Rule**: "If Temp > 80 AND Smoke > 20% over a 5s window, trigger warning."

---

## 8. Smart Building Reference Implementation

Explore `examples/smart_building/` to see **CEP in action**:
1.  **Start Engine**: `python app.py --log INFO`
2.  **CEP Demo**: Trigger "High Temp" in the generator. Notice how the Tactical path waits for a trend in the telemetry window before firing.

---

## 9. Publishing to PyPI

### 9.1 Automated Publishing
Uses the provided `.github/workflows/publish.yml` on every GitHub Release.

### 9.2 Setup
Register at [pypi.org](https://pypi.org) and enable "Trusted Publishing" for the `balamuru/knwstack` repository.
