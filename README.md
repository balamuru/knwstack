![knwstack](/assets/images/logo.png)
# KnwStack
**Pronounced "know-stack" ( /noʊ-stæk/ ).**
A high-performance, low-latency reference architecture for real-time AI inference. Bridging the gap between "knowing" and "now."

## Overview

KnwStack is an opinionated Complex Event Processing (CEP) and AI routing framework. It implements the **"Split-Brain"** architecture to solve the fundamental tension between low-latency physical reflexes and high-latency LLM intellect.

Traditional AI agent frameworks are too slow to react to high-frequency telemetry, while traditional stream processors lack the native semantic reasoning of LLMs. KnwStack bridges this gap by routing continuous data streams through latency-specific execution paths.

## Inspiration & The Need

The industry currently treats Real-Time Event Processing and Generative AI as two isolated domains. Standard AI frameworks (like LangChain or LlamaIndex) are inherently Request-Response oriented; if you attempt to feed a 60Hz telemetry stream directly into them, they will immediately collapse under extreme latency and API rate limits. 

Conversely, enterprise stream-processing engines are incredibly fast but rigidly deterministic; they cannot natively reason about unstructured data or explain complex anomalies on the fly. 

**KnwStack was born from the need to unite these two paradigms.** There was a critical lack of tooling that allowed an AI agent to "reflexively" react to immediate hazards in milliseconds, while simultaneously capturing the rolling context required to send a reasoned analysis to an LLM seconds later. By formalizing the "Split-Brain" pattern into an open-source framework, KnwStack gives developers the exact tools needed to build Trustable, Real-Time AI systems for physical, financial, and high-frequency environments.

## Gap Analysis: KnwStack vs. Current Tech

| Technology / Pattern | The "Gap" (What's Missing) | How KnwStack Solves It |
| :--- | :--- | :--- |
| **LangChain / LlamaIndex** | Built for stateful, request-response loops. Chokes on high-frequency continuous data (e.g., 60Hz telemetry). | KnwStack is **event-driven**. It buffers data and triggers the LLM asynchronously (harnessing **LiteLLM** as a lightweight, multi-provider LLM router) only when specific anomalies are detected, preventing rate-limit collapses. |
| **Apache Flink / Kafka Streams** | Excellent for sub-10ms streaming, but entirely deterministic. Cannot easily integrate non-deterministic LLMs inline without causing massive backpressure. | KnwStack implements a **Multi-Tier Router** powered by **Bytewax**. It runs deterministic rules on the Hot Path immediately, while offloading LLM calls to a non-blocking Cold Path. |
| **Confluent Platform / Flink SQL** | *Can* call LLMs via Flink SQL connectors, but it remains infrastructure plumbing. Developers must manually architect the asynchronous logic, timeouts, and dual-paths in complex SQL or Java. | KnwStack provides an opinionated **Application-Layer Framework**. It gives developers simple Python abstractions (e.g., `@strategic_prompt`) that automatically handle the async LLM orchestration and cooldowns under the hood. |
| **Standard Microservices** | Querying a database for historical context before every LLM call adds significant latency. | KnwStack maintains a rolling **In-Memory Context Window**. Handled natively by **Bytewax's** stateful windowing, the LLM immediately gets the last X seconds of data injected without manual cache management or database lookups. |

## Key Features

*   **Multi-Tier Routing:**
    *   ⚡ **Reflex Path (Hot):** Sub-10ms deterministic execution for immediate physical actions.
    *   🧠 **Tactical Path (Warm):** Sub-100ms execution utilizing fast, localized ML models.
    *   🤖 **Strategic Path (Cold):** Asynchronous, high-latency execution for deep Cloud LLM reasoning.
*   **Multi-Tenant Concurrency:** Safely process multiple independent use cases (e.g., weather telemetry and financial ticks) concurrently on the same infrastructure using strict topic isolation.
*   **Stateful CEP Joins:** Synchronize and aggregate multiple streams (e.g., temperature + wind) within precise time windows before triggering rules or prompts.
*   **MCP Tool Integration:** The Strategic Path supports the **Model Context Protocol (MCP)**. LLMs can dynamically call external tools (databases, APIs, internal services) during their asynchronous reasoning loop to gather missing context before returning a decision to the stream.
*   **Synthesis & Feedback Loops:** KnwStack natively supports routing actions back into itself. The Bytewax engine can consume both the immediate Reflex actions and the delayed Strategic LLM reasoning to form a unified, correlated synthesis decision.
*   **Developer-First Abstractions:** Write high-performance routing logic in pure Python using simple decorators (`@reflex_rule`, `@strategic_prompt`).

## The Technology Stack

KnwStack prioritizes extreme performance and a premium AI developer experience by combining the speed of Rust and Go with the ecosystem of Python.

*   **Messaging Backbone:** [NATS JetStream](https://nats.io/) (Ultra-lightweight, exact-once delivery, Go)
*   **Stream Engine & State:** [Bytewax](https://bytewax.io/) (Python API over Rust's Timely Dataflow)
*   **AI Orchestration:** [LiteLLM](https://docs.litellm.ai/) (Acts as a lightweight LLM API router, providing a unified interface to 100+ LLMs without the overhead of heavy prompt-chaining libraries)

## Architecture

Read the full [Architecture Design Document](docs/architecture.md) for detailed sequence diagrams, component breakdowns, and the rationale behind our design decisions.

## NATS Infrastructure Setup

KnwStack relies on **NATS JetStream** for high-performance, persistent messaging.

### 1. Installation
The easiest way to get started is via **Docker Compose**. Alternatively, you can download the binary for your OS.

*   **Docker (Recommended):** Ensure you have [Docker](https://docs.docker.com/get-docker/) installed.
*   **Manual Binary:** Download the `nats-server` and `nats` CLI from the [Official NATS Download Page](https://nats.io/download/).

### 2. Start the NATS Server

#### Option A: Docker Compose (Recommended)
Run the following command from the project root to start the NATS infrastructure:

```bash
docker compose up -d
```

This starts:
- **NATS Server**: Port `4222`
- **NATS Monitoring**: [http://localhost:8222](http://localhost:8222)
- **NATS Management UI (NUI)**: [http://localhost:31311](http://localhost:31311) — Use this to visually manage streams, consumers, and messages.

#### Option B: Manual Binary
If running locally without Docker, start the server with JetStream enabled:
```bash
nats-server -js
```

**Example `nats.conf`:**
```text
jetstream {
    store_dir: "/data/nats-jetstream"
    max_mem: 1G
    max_file: 10G
}
```

### 2. Configure JetStream Streams
KnwStack requires a Stream to be defined for the subjects it consumes. You can create this stream using the Web UI or the `nats` CLI.

#### Option A: Using NUI (Web Interface)
If using Docker Compose, this is the easiest method.
1. Navigate to [http://localhost:31311](http://localhost:31311)
2. Go to **JetStream** -> **Streams** -> **Add Stream**
3. Name it `KNWSTACK_STREAM` and add the subject `app.>`

#### Option B: Using Docker CLI
If you are running the Docker Compose setup but prefer the terminal, you can use the official `nats-box` utility container, which comes pre-installed with the CLI and connects to your Docker network:

```bash
docker compose exec nats-box nats stream add KNWSTACK_STREAM \
    --subjects "app.>" \
    --storage file \
    --retention limits \
    --discard old \
    --max-msgs=-1 \
    --max-bytes=-1 \
    --max-age=1h \
    --dupe-window=2m \
    --defaults
```

#### Option C: Using Local CLI
If you installed the `nats` CLI binary locally, simply run:
```bash
nats stream add KNWSTACK_STREAM \
    --subjects "app.>" \
    --storage file \
    --retention limits \
    --discard old \
    --max-msgs=-1 \
    --max-bytes=-1 \
    --max-age=1h \
    --dupe-window=2m \
    --defaults
```

### 3. Messaging Patterns
*   **Subject Hierarchy:** KnwStack uses dot-separated subjects (e.g., `tenant1.weather.temp`). The first token is always treated as the **Tenant ID** for state isolation.
*   **Load Balancing:** The framework utilizes NATS **Queue Groups** (`knwstack_workers`) by default. This allows you to scale the engine horizontally by simply spinning up more Bytewax workers; NATS will automatically balance the incoming events across them.

## Getting Started

### 1. Installation
KnwStack uses `uv` for lightning-fast dependency management.

```bash
# Clone the repository
git clone https://github.com/balamuru/knwstack.git
cd knwstack

# Install dependencies
uv sync
```

### 2. Execution Environments
KnwStack uses a virtual environment (located in `.venv/`) to isolate dependencies. There are two ways to run commands:

*   **Option A: Using `uv run` (Recommended):**
    Run any command prefixed with `uv run`. This automatically ensures the environment is in sync with `pyproject.toml` and executes the command within the virtual environment.
    ```bash
    uv run python -m knwstack.engine
    ```
*   **Option B: Manual Activation:**
    If you prefer the traditional workflow, activate the environment once per terminal session:
    ```bash
    source .venv/bin/activate
    python -m knwstack.engine
    ```

### 3. Running an Application
To run a specific application (like the Smart Building example), navigate to the application directory and use the Bytewax runner:
```bash
cd examples/smart_building
uv run python -m bytewax.run app:flow
```

## Directory Structure

```text
knwstack/
├── docs/                 # Architecture and design documentation
├── scripts/              # Developer utilities (App Scaffolder, Event Injector)
├── src/
│   └── knwstack/
│       ├── api/          # Developer abstractions (@reflex_rule, etc.)
│       ├── connectors/   # NATS JetStream input/output operators
│       ├── engine/       # Core Bytewax dataflow and multi-tier router
│       └── state/        # CEP windowing and state management helpers
├── pyproject.toml        # Project dependencies
└── README.md
```
