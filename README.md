<p align="center">
  <img src="assets/images/logo.png" width="300" alt="KnwStack Logo">
</p>

# KnwStack

**The Real-Time AI Framework for "Split-Brain" Streaming.**

KnwStack allows you to process high-velocity data streams while simultaneously running deep LLM reasoning. It implements a unique **"Split-Brain"** architecture that separates sub-10ms Reflexes from seconds-latency Strategic AI.

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
│       ├── engine/       # Core Pathway Routing Engine
│       └── connectors/   # NATS JetStream Adapters
├── tests/                # Comprehensive Test Suite
└── examples/             # Reference Implementations
```

---

*Built with Pathway & NATS.*
