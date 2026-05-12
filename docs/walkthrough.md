# KnwStack Technical Deep Dive
*A Code-Level Walkthrough of the Split-Brain Engine*

This document explores the internal implementation of KnwStack. It is intended for developers who want to understand how Python decorators are transformed into a high-performance, incremental dataflow.

---

## 1. The Registry Bridge (`api/decorators.py`)

The core of the developer experience is the `RuleRegistry`. Unlike many frameworks that use complex inheritance, KnwStack uses a simple metadata store.

### 1.1 Metadata Storage
When you use `@reflex_rule`, the decorator doesn't wrap the function in a complex class. Instead, it registers the function and its subject filter into a global dictionary:
```python
# Internal structure
self.reflex_rules = {
    "telemetry.>": [func1, func2],
    "alarms.*": [func3]
}
```

### 1.2 Test Isolation Mechanics
In `decorators.py`, we implemented a `clear()` method. This is critical because `pytest` runs in a single process by default. Without this, rules from `test_a.py` would leak into `test_b.py`. We use an **autouse fixture** in `tests/conftest.py` to trigger this clear before every single test.

---

## 2. The Engine Dataflow (`engine/router.py`)

The `run_engine` function is where the "Split-Brain" is physically constructed. It follows a **Broadcast & Branch** pattern.

### 2.1 The Ingestion Loop
We use `pw.io.python.read` combined with our custom `NatsSource`. This is a low-level bridge that runs a polling loop inside a background thread, yielding messages to Pathway. This prevents NATS blocking from affecting the computation speed.

### 2.2 Branch 1: The Hot Path (Reflex)
Implemented in `apply_reflex`. This uses `pw.apply`.
*   **Why `pw.apply`?** It is a stateless operation. It takes one row and returns one row. This is the fastest possible path in Pathway because there is no windowing or state-shuffling involved.

### 2.3 Branch 2: The Warm Path (Tactical)
Implemented in `run_tactical`. This is where the complexity increases.
*   **Windowing**: We use `pw.temporal.sliding(duration=length_s, step=slide_s)`. Pathway's engine maintains an incremental buffer of these events.
*   **Aggregation**: We use `.reduce()` to group events by their key (e.g., `building_id`). This is how we support **Multi-Pod Correlation**—Pathway ensures the same key always lands in the same window.

### 2.4 Branch 3: The Cold Path (Strategic)
Implemented in `run_strategic`.
*   **The Async Paradox**: Pathway is a synchronous incremental engine. If we called an LLM inside the dataflow, the entire engine would freeze.
*   **The Solution**: We use a **Split-Handshake**. The engine prepares the prompt and emits it to a background `ThreadPoolExecutor`. The engine continues processing while the LLM reasoning happens out-of-band.

---

## 3. NATS Connector (`connectors/nats.py`)

Our NATS implementation solves the **Pull-to-Stream** problem.

*   **JetStream Pull**: We use a `fetch()` loop that requests small batches of messages from NATS.
*   **Subject Filtering**: The connector automatically maps NATS hierarchy into a flat data structure that Pathway can ingest as a table with columns like `subject`, `payload`, and `timestamp`.

---

## 4. Operational Narrative (Observability)

The "Narrative" logs are implemented using a custom `logging` wrapper that inspects the `path_type` of the rule being executed. This allows us to inject the `⚡ [INGEST]` and `🟠 [WARM]` labels at the precise moment the router dispatches a rule, providing a visual trace of the "Split-Brain" in action.

---
*Technical Deep Dive: May 2026*
