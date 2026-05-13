![KnwStack Logo](assets/images/logo.png)


# KnwStack
*(Pronounced: **"Know-Stack"** — Where Knowledge meets Now)*

**The Real-Time AI Framework for "Split-Brain" Streaming.**

KnwStack allows you to process high-velocity data streams while simultaneously running deep LLM reasoning. It implements a unique **"Split-Brain"** architecture that separates sub-10ms Reflexes from seconds-latency Strategic AI.

---

## ⚡ Why KnwStack?

Traditional streaming (Kafka/Flink) is built for throughput, not intelligence. Traditional AI agents (LangChain) are built for reasoning, not speed. **KnwStack is the bridge.**

*   **Sub-10ms Reflexes**: Deterministic rules that fire instantly when an anomaly is detected.
*   **Asynchronous Intellect**: Deep LLM analysis that runs in parallel without blocking the critical path.
*   **Pure Python**: Build complex real-time AI pipelines using simple decorators.
*   **Lightweight**: Powered by NATS and Pathway—no JVM, no Zookeeper, no bloat.

---

## 🎯 Use Cases

*   **Industrial IoT**: Shut down a machine in 5ms while an LLM diagnoses the vibration patterns.
*   **Smart Infrastructure**: React to floor alarms instantly while reasoning about building-wide security protocols.
*   **FinTech**: Execute local risk-checks on market ticks while a Strategic path plans long-term hedging.

### 🚫 What it is NOT for
*   **Data Lakes**: If you need to archive petabytes of history for years, use Kafka or S3.
*   **Batch Processing**: If you are calculating 3-year averages for 1M sensors, use Spark or Polars.
*   **Simple CRUD**: For standard web forms and profile pages, use FastAPI or Django.

### ⚖️ Comparison

| Feature | KnwStack | Confluent (Kafka) | Redpanda | LangChain |
| :--- | :--- | :--- | :--- | :--- |
| **Primary Goal** | **AI Routing** | Data Persistence | Fast Ingestion | Agent Logic |
| **Split-Brain** | **Native (Hot/Cold)** | ❌ (Manual Setup) | ❌ (Manual Setup) | ❌ (Synchronous) |
| **Latency Focus** | **Sub-10ms** | 100ms+ | 10ms - 50ms | Seconds (API) |
| **Infrastructure** | Single Binary (NATS) | Complex (JVM/Zoo) | Single Binary | Library |
| **Programming** | Python Decorators | Java/Scala Heavy | Kafka API | Chains/Graphs |

---

## 🚀 Quick Start

### 1. Infrastructure
Spin up the NATS backbone:
```bash
docker compose up -d
```

### 2. Environment
Setup and activate using `uv`:
```bash
uv sync
source .venv/bin/activate
```

### 3. Run the Reference Implementation
Explore the Smart Building example:
```bash
cd examples/smart_building
python app.py --log INFO
```

**Optional: Enable Monitoring**
To enable the background metrics server:
```bash
python app.py --dashboard --port 9090
```
Then view metrics at: `http://localhost:9090/metrics`

In a second terminal, trigger events with the interactive generator:
```bash
python generator.py
```

---

## 📖 Documentation & Resources

For deep dives on architecture, design decisions, and full API documentation, see the **[KnwStack Handbook](docs/handbook.md)**.

### Key Sections:
*   **[Split-Brain Architecture](docs/handbook.md#1-the-split-brain-architecture)**: How we decouple speed from intelligence.
*   **[Why this Stack?](docs/handbook.md#2-why-this-stack-design-decisions)**: Why we chose NATS, Pathway, and LiteLLM.
*   **[Developer API](docs/handbook.md#3-developer-api-reference)**: Using decorators to build your first app.
*   **[Testing Guide](docs/handbook.md#5-testing--quality-assurance)**: Running the automated test suite.

---

## Directory Structure

```text
knwstack/
├── docs/                 # Handbook and Technical Guides
├── scripts/              # Developer utilities
├── src/
│   └── knwstack/
│       ├── api/          # Decorators and Registries
│       ├── engine/       # Core Engine (Graph) & Runner (Lifecycle)
│       └── connectors/   # NATS JetStream Adapters
├── tests/                # Comprehensive Test Suite
└── examples/             # Reference Implementations
```

---

*Built with Pathway & NATS.*
