---
name: knwstack-doc-reviewer
description: Specialized skill for reviewing KnwStack documentation. Ensures consistency with the codebase, technical accuracy, and adherence to documentation standards.
---

# KnwStack Documentation Reviewer Skill

## When to use this skill
- Use this when documentation has been created or updated.
- Use this to verify that setup instructions (like NATS configuration) are technically correct and complete.

## Review Checklist
1. **Code Alignment**: Does the documentation accurately reflect the current code implementation (e.g., subjects, connectors, decorators)?
2. **N-Path Consistency**: Ensure the Reflex, Tactical, and Strategic paths are correctly defined and distinguished.
3. **External Dependencies**: Check that third-party library roles (Bytewax, NATS, LiteLLM) are correctly attributed and not confused with core framework logic.
4. **Setup Completeness**: Verify that all necessary infrastructure steps (e.g., JetStream enabling, stream creation) are documented.
5. **Diagram Accuracy**: Check Mermaid diagrams for syntax errors and logical flow consistency.
6. **Tone & Style**: Ensure the tone is professional, technical, and uses GitHub alerts effectively.
