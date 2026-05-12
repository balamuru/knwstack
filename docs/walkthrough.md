# KnwStack Framework: Technical Walkthrough

KnwStack is a high-performance Real-Time AI framework that enables the "Split-Brain" architecture. This document provides a deep dive into the implementation details of the core engine and developer abstractions.

## 1. Project Initialization & Dependencies

The codebase utilizes modern, high-performance dependencies. The core logic is structured neatly under `src/knwstack`:

*   **`api/`**: Developer abstractions and decorator registries.
*   **`connectors/`**: Native and custom Pathway connectors (e.g., NATS).
*   **`engine/router.py`**: The Pathway Core Router and Multi-Tier Dataflow.

## 2. The Developer API (Decorators)

In `api/decorators.py`, we implemented a global registry system. This allows developers to tag their Python functions with `@reflex_rule`, `@tactical_model`, or `@strategic_prompt`.

> All rules are aggregated into a global registry that the Pathway engine automatically ingests when the dataflow starts. This separation of "Logic" (the rules) from "Engine" (the dataflow) is what makes KnwStack so flexible.

## 3. The Core Pathway Engine & Multi-Tier Routing

The heart of KnwStack is in `engine/router.py`. We utilize **Pathway**, a high-performance Rust-backed streaming engine, to build the dataflow.

### 3.1 Custom NATS Ingestion
We implemented a custom `NatsSource` in `connectors/nats_connector.py` that utilizes `pw.io.python.read`. This gives us full control over message metadata (like subjects) and ensures stable, deterministic ingestion which is critical for Pathway's incremental engine.

### 3.2 Tiered Processing Paths
The engine splits incoming events into three distinct Pathway streams:

1.  **Reflex Path (Hot):** Uses `pw.apply` to execute deterministic rules instantly. Results are pushed back to NATS as soon as they are computed.
2.  **Tactical Path (Warm):** Leverages Pathway's native `windowby` and `reduce` operations to aggregate telemetry over sliding windows (e.g., 5-second rolling averages) before executing models.
3.  **Strategic Path (Cold):** Also uses windowing to aggregate context, but then offloads the prompt construction to an asynchronous background worker using `litellm`. This ensures that slow LLM network calls never block the high-speed Reflex path.

## 4. Verification & Testing

We verified the architecture using the **Smart Building Example** (`examples/smart_building/app.py`). 

### Test Results:
*   **Reflex Success:** Triggered a `fire` alarm via `scripts/injector.py` and observed a sub-10ms shutdown action published to NATS.
*   **Tactical Success:** Injected a series of high-temperature telemetry events and confirmed the sliding window correctly triggered a cooling action based on rolling averages.
*   **Reliability:** The transition from Bytewax's `Timely Dataflow` to Pathway's `Incremental Computation` model resulted in simpler code and better handling of late-arriving events.

---
*Documentation updated: May 2026 (Pathway Migration Finalized)*
