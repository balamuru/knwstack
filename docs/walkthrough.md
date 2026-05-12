# KnwStack Framework: Technical Walkthrough
*Professionalization & Production Readiness Milestone*

KnwStack is a high-performance Real-Time AI framework that enables the "Split-Brain" architecture. This document provides a deep dive into the implementation details of the core engine, developer abstractions, and the infrastructure that ensures production stability.

---

## 1. Project Initialization & Dependencies

The codebase utilizes modern, high-performance dependencies managed by **`uv`**. The core logic is structured neatly under `src/knwstack`:

*   **`api/`**: Developer abstractions and decorator registries.
*   **`connectors/`**: Native and custom Pathway connectors (e.g., NATS).
*   **`engine/router.py`**: The Pathway Core Router and Multi-Tier Dataflow.
*   **`cli/`**: Tooling for scaffolding and management.

---

## 2. The Developer API (Decorators)

In `api/decorators.py`, we implemented a global registry system. This allows developers to tag their Python functions with `@reflex_rule`, `@tactical_model`, or `@strategic_prompt`.

> **Rule Registry Isolation**: To support robust unit testing, we implemented a `RuleRegistry.clear()` method and an autouse fixture in `tests/conftest.py`. This ensures every test run starts with a clean slate, preventing state leakage between test cases.

---

## 3. The Core Pathway Engine & Multi-Tier Routing

The heart of KnwStack is in `engine/router.py`. We utilize **Pathway**, a high-performance Rust-backed streaming engine, to build the dataflow.

### 3.1 Custom NATS Ingestion
We implemented a custom `NatsSource` that utilizes `pw.io.python.read`. This gives us full control over message metadata (like subjects) and ensures stable, deterministic ingestion which is critical for Pathway's incremental engine.

### 3.2 Tiered Processing Paths
The engine splits incoming events into three distinct Pathway streams:
1.  **Reflex Path (Hot):** Sub-10ms deterministic rules.
2.  **Tactical Path (Warm):** Windowed heuristics and CEP (Complex Event Processing).
3.  **Strategic Path (Cold):** Asynchronous LLM reasoning via `litellm`.

---

## 4. Observability Narrative

We refactored the engine's internal routing into testable helpers (`apply_reflex`, `run_tactical`, `run_strategic`) and introduced a standardized logging narrative. 

Developers can now "see" the brain think in real-time using color-coded labels:
*   `⚡ [INGEST]`: Entry point.
*   `   ∟ [HOT]`: Immediate reflex evaluation.
*   `🟠 [WARM]`: Windowed trend analysis (CEP).
*   `🔵 [COLD]`: Strategic reasoning dispatch.

---

## 5. CI/CD & Production Infrastructure

### 5.1 Automated Testing (10/10 Passing)
We established a comprehensive test suite covering everything from API decorators to the internal routing logic. The framework maintains 100% pass rates across its core functionality.

### 5.2 GitHub Actions Pipeline
A professional CI/CD pipeline was implemented in `.github/workflows/tests.yml`. 
*   **Performance**: Uses `astral-sh/setup-uv` for lightning-fast dependency resolution.
*   **Stability**: Resolves complex `pyarrow` build issues by using Python 3.12 and explicit Apache Arrow C++ headers.
*   **Modern Runtime**: Opts into Node.js 24 to future-proof the workflow.

### 5.3 PyPI Publishing Ready
The project is configured for automated publishing via **Trusted Publishing (OIDC)**. A dedicated `publish.yml` workflow triggers on new GitHub Releases, building and pushing the library to PyPI securely without manual credentials.

---

## 6. Verification
We verified the architecture using the **Smart Building Example** (`examples/smart_building/`). 
*   **Interactive Testing**: Users can use the `generator.py` CLI to trigger all 4 event types (Nominal, Hot, Warm, Cold) and observe the engine's response in real-time.
*   **Scaling Logic**: The framework is documented to scale horizontally using NATS Durable Consumer Groups and key-based partitioning.

---
*Documentation updated: May 2026 (Professionalization Milestone)*
