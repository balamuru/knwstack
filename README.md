![knwstack](/assets/images/logo.png)
# KnwStack
knwstack: A high-performance, low-latency reference architecture for real-time AI inference. Bridging the gap between "knowing" and "now."

## Overview

KnwStack is an opinionated Complex Event Processing (CEP) and AI routing framework. It implements the **"Split-Brain"** architecture to solve the fundamental tension between low-latency physical reflexes and high-latency LLM intellect.

Traditional AI agent frameworks are too slow to react to high-frequency telemetry, while traditional stream processors lack the native semantic reasoning of LLMs. KnwStack bridges this gap by routing continuous data streams through latency-specific execution paths.

## Key Features

*   **Multi-Tier Routing:**
    *   ⚡ **Reflex Path (Hot):** Sub-10ms deterministic execution for immediate physical actions.
    *   🧠 **Tactical Path (Warm):** Sub-100ms execution utilizing fast, localized ML models.
    *   🤖 **Strategic Path (Cold):** Asynchronous, high-latency execution for deep Cloud LLM reasoning.
*   **Multi-Tenant Concurrency:** Safely process multiple independent use cases (e.g., weather telemetry and financial ticks) concurrently on the same infrastructure using strict topic isolation.
*   **Stateful CEP Joins:** Synchronize and aggregate multiple streams (e.g., temperature + wind) within precise time windows before triggering rules or prompts.
*   **Developer-First Abstractions:** Write high-performance routing logic in pure Python using simple decorators (`@reflex_rule`, `@strategic_prompt`).

## The Technology Stack

KnwStack prioritizes extreme performance and a premium AI developer experience by combining the speed of Rust and Go with the ecosystem of Python.

*   **Messaging Backbone:** [NATS JetStream](https://nats.io/) (Ultra-lightweight, exact-once delivery, Go)
*   **Stream Engine & State:** [Bytewax](https://bytewax.io/) (Python API over Rust's Timely Dataflow)
*   **AI Orchestration:** [LiteLLM](https://docs.litellm.ai/) (Unified interface for 100+ LLMs)

## Architecture

Read the full [Architecture Design Document](docs/architecture.md) for detailed sequence diagrams, component breakdowns, and the rationale behind our design decisions.

## Getting Started

*(Installation and usage instructions coming soon as the framework is actively implemented)*

```bash
# Clone the repository
git clone https://github.com/balamuru/knwstack.git
cd knwstack

# Install dependencies (using uv)
uv sync

# Start the local NATS broker
nats-server -js

# Run the KnwStack engine
python -m knwstack.engine
```

## Directory Structure

```text
knwstack/
├── docs/                 # Architecture and design documentation
├── src/
│   └── knwstack/
│       ├── api/          # Developer abstractions (@reflex_rule, etc.)
│       ├── connectors/   # NATS JetStream input/output operators
│       ├── engine/       # Core Bytewax dataflow and multi-tier router
│       └── state/        # CEP windowing and state management helpers
├── pyproject.toml        # Project dependencies
└── README.md
```
