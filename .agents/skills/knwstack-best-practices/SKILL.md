---
name: knwstack-best-practices
description: Core architectural patterns, conventions, and best practices for the KnwStack framework. Use this when writing new features, debugging, or reviewing code in the knwstack project.
---

# KnwStack Best Practices & Architecture Guide

## When to use this skill

- Use this when modifying the core framework, adding new streams, or architecting new features.
- Use this during code generation to ensure alignment with KnwStack conventions.

## 1. Core Paradigm: "Split-Brain" Architecture
KnwStack separates continuous data streams into latency-specific execution paths to handle the tension between fast reflexes and slow LLM logic:
- **Reflex Path (Hot)**: Sub-10ms deterministic execution for immediate physical actions. Must not be blocked.
- **Tactical Path (Warm)**: Sub-100ms execution for fast local ML inference.
- **Strategic Path (Cold)**: Asynchronous, seconds-latency execution for deep Cloud LLM reasoning. LLM network calls must use non-blocking `asyncio` tasks so they never block the Hot Path.

## 2. Technology Stack & Key Decisions
- **Messaging (NATS JetStream)**: Used instead of Apache Kafka for its lightweight nature, exact-once delivery, and subject-based routing (`tenant.app.event`) which enables easy multi-tenancy. Load balancing is achieved via NATS Queue Groups.
- **Stream Engine (Bytewax)**: Used instead of Apache Flink. Provides Rust-level performance and memory safety with a pure Python Developer API. Handles stateful windowing (CEP Joins) natively.
- **AI Orchestration (LiteLLM)**: Used instead of heavy frameworks like LangChain. Keeps the Cold Path lightweight and provider-agnostic, easily routing to 100+ LLMs.
- **Dependency Management**: Uses `uv` (as defined in `pyproject.toml`).

## 3. Developer API Abstractions
Developers write high-performance logic using simple decorators that register logic globally for the Bytewax engine:
- `@reflex_rule("topic.name")`
- `@tactical_model("topic.name")`
- `@strategic_prompt("topic.name", cooldown_s=60)`

## 4. Project Structure Conventions
Maintain logic isolation following the established directory structure:
- `docs/`: Architecture and implementation walkthroughs
- `scripts/`: CLI utilities (Scaffolding, Test Event Injection)
- `src/knwstack/api/`: Developer abstractions and decorators
- `src/knwstack/connectors/`: NATS JetStream I/O operators
- `src/knwstack/engine/`: Core Bytewax dataflow and the multi-tier router
- `src/knwstack/state/`: CEP windowing configurations
