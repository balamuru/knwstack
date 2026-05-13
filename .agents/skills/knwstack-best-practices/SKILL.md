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
- **Reflex Path (Hot)**: Sub-10ms deterministic execution for immediate physical actions. Must be stateless and deterministic.
- **Tactical Path (Warm)**: Sub-100ms execution for windowed heuristics and Complex Event Processing (CEP).
- **Strategic Path (Cold)**: Asynchronous, seconds-latency execution for deep LLM reasoning. LLM calls must be bridged from Pathway's synchronous engine without blocking the dataflow.

## 2. Technology Stack & Key Decisions
- **Messaging (NATS JetStream)**: Lightweight, binary-based messaging. Supports both sub-millisecond Core Push (Reflex) and reliable JetStream Pull (Tactical).
- **Stream Engine (Pathway)**: Replaced Bytewax. Provides an incremental computation engine that allows Python to be compiled into a high-performance Rust dataflow. 
- **AI Orchestration (LiteLLM)**: Keeps the Strategic path lightweight and provider-agnostic, supporting 100+ LLMs with a unified API.
- **Dependency Management**: Standardized on `uv` for speed and deterministic builds.

## 3. Ingestion Modes (Push-to-Pull Bridge)
Pathway expects a pull-based (polled) data source. To support NATS:
- **JetStream Pull**: Use the native `fetch()` API for persistent streams.
- **Core NATS Bridge**: For "SuperHot" Push subjects, use a background thread with a thread-safe queue. The connector subscribes (Push) and then bridges into Pathway's `next()` (Pull) interface.

## 4. Framework Implementation "Gotchas" (Critical)
- **Synchronous Computation**: Pathway's `pw.apply` and `reduce` operations are synchronous. LLM calls in the Strategic path MUST be made using the synchronous `litellm.completion` or a properly managed async bridge to prevent blocking the entire engine.
- **Subject Matching**: In the Reflex path, use the internal `match_subject` helper to support NATS wildcards (`*` and `>`). Do not use exact string equality for subjects.
- **Test Isolation**: Always use `RuleRegistry.clear()` between test runs to prevent rules from leaking across unit tests. Use the autouse fixture in `conftest.py`.

## 5. Developer API Abstractions
Developers write logic using simple decorators that register metadata for the Pathway engine:
- `@reflex_rule("topic.name")`
- `@tactical_model("topic.name", window_type="sliding", length_s=5)`
- `@strategic_prompt("topic.name", cooldown_s=60)`

## 6. Project Structure Conventions
- `docs/`: Handbook and Technical Deep Dives.
- `src/knwstack/api/`: Decorators and the Rule Registry.
- `src/knwstack/engine/`: The Pathway router and "Split-Brain" logic.
- `src/knwstack/connectors/`: NATS I/O adapters.
- `tests/`: Comprehensive test suite (Unit and Integration).
