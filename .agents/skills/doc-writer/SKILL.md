---
name: knwstack-doc-writer
description: Specialized skill for creating high-quality, technically accurate documentation for the KnwStack framework. Use when writing READMEs, architecture docs, or walkthroughs.
---

# KnwStack Documentation Writer Skill

## When to use this skill
- Use this when the user asks to document a new feature, component, or the overall architecture.
- Use this to ensure all documentation follows the KnwStack "Split-Brain" terminology and design patterns.

## Writing Standards
1. **Terminology**: Strictly use the standard paths: **Reflex (Hot)**, **Tactical (Warm)**, and **Strategic (Cold)**.
2. **Visuals**: Always include Mermaid diagrams for complex flows, sequence diagrams for event lifecycles, and tables for comparisons.
3. **Clarity**: Use GitHub alerts (`> [!NOTE]`, `> [!TIP]`, etc.) to highlight critical configuration details or architectural nuances.
4. **Third-Party Attribution**: Explicitly mention when features are "harnessed" or "powered by" external libraries (e.g., Bytewax, NATS, LiteLLM).

## Document Structure
- **Executive Summary**: Brief "What" and "Why".
- **Architecture Diagram**: Visual overview.
- **Component Breakdown**: Deep dive into individual modules.
- **Setup & Config**: Thorough, copy-pasteable instructions.
- **Walkthrough/Examples**: Practical code snippets showing usage.
