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
- **Subject Matching**: Use hierarchical wildcards (`>`) in decorators to support multi-level subjects (e.g., `bldg1.hvac.telemetry`). The engine MUST subscribe to `>` in the NATS connector to ensure all relevant traffic is captured.
- **Multi-Tenant Isolation**: Always use a `key` field in telemetry data. Use the `groupby("key")` pattern in Pathway windows to ensure independent processing per tenant/building. If a `key` is missing, fallback to the first segment of the NATS subject.
- **Temporal Synchronization (Heartbeats)**: In low-traffic or intermittent streams, windows may stall. Always inject a periodic `heartbeat` event into the stream to force Pathway's watermark forward and ensure windows close reliably.
- **Schema Flexibility**: Extraction UDFs (like `get_time` or `get_key`) MUST handle both Python `dict` and Pathway's `Json` wrapper types to remain resilient against heterogeneous input formats.

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
