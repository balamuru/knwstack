# KnwStack: Reference Architecture Implementation Walkthrough

I have successfully initialized and scaffolded the `knwstack` repository, implementing the core components of the "Split-Brain" Real-Time AI framework.

## Project Structure Created
The codebase has been initialized with `pyproject.toml` utilizing modern dependencies (`bytewax`, `nats-py`, `litellm`, `pydantic`). The core logic is structured neatly under `src/knwstack`:

*   **`api/decorators.py`**: The Developer API.
*   **`engine/router.py`**: The Bytewax Core Dataflow.
*   **`connectors/nats.py`**: NATS JetStream integration.
*   **`state/windowing.py`**: CEP windowing configurations.

---

## 1. The Developer API

I implemented the declarative `RuleRegistry` and the N-Path decorators to make defining real-time AI agents extremely simple for the end-user.

Users can define deterministic reflexes:
```python
@reflex_rule("weather.temp")
def shutdown_reflex(events):
    # Triggers immediately in <10ms if rule is met
```

And define Strategic AI prompts for the same dataset:
```python
@strategic_prompt("finance.tick", cooldown_s=60)
def analyze_market_anomaly(events):
    # Constructs the context for LiteLLM to orchestrate
```

> [!NOTE]
> All rules are aggregated into a global registry that the Bytewax engine automatically ingests when the dataflow starts.

---

## 2. Messaging Integration (NATS JetStream)

In `connectors/nats.py`, I built the Bytewax input and output operators using `nats-py`.

> [!TIP]
> **Multi-Tenant Load Balancing:** The `NatsSourcePartition` explicitly subscribes using a NATS **Queue Group** (`knwstack_workers`). This ensures that as you spin up multiple instances of the framework, incoming events are perfectly load-balanced across your cluster without any manual coordination.

---

## 3. The Core Bytewax Engine & CEP Joins

The true brain of the framework lives in `engine/router.py`. Here is how the event lifecycle executes:

1.  **Ingestion & Keying:** Events arrive from NATS. I implemented an `extract_tenant_key` function that uses the NATS subject prefix (e.g., `tenant1` from `tenant1.weather.temp`) to isolate the state.
2.  **CEP Windowing:** The stream passes through a `TumblingWindow` (configured in `state/windowing.py`) which aggregates all events for a specific tenant occurring within that time slice (e.g., 1 second). This natively handles **Cross-Stream Aggregation**.
3.  **The Multi-Tier Router:**
    *   **Hot & Warm Paths:** The windowed events are passed to the synchronous functions registered via `@reflex_rule` and `@tactical_model`. Actions are immediately published back to NATS.
    *   **Cold Path:** If a `@strategic_prompt` is triggered, the engine spawns a non-blocking `asyncio` task (`_execute_strategic_async`) to handle the LiteLLM network call. This guarantees that a slow GPT-4 call will *never* block the Hot Path reflexes.

---

## 4. Testing & Validation

I created `tests/test_router.py` which demonstrates a multi-tenant setup:
*   A Reflex rule shutting down a system on high temps (`weather.temp`).
*   A Tactical ML classification on wind speed (`weather.wind`).
*   A Strategic LLM prompt triggered by market crashes (`finance.tick`).

The tests successfully validate that the registry properly tracks topic assignments and isolates the N-Path definitions.
