---
name: knwstack-injector
description: Publishes mock JSON telemetry directly into NATS JetStream. Use when you need to test the KnwStack engine or simulate events.
---

# KnwStack Test Event Injector

## When to use this skill

- Use this when the user asks to test the knwstack engine.
- Use this to simulate an event or publish test telemetry.

## How to use it

When triggered, use the `scripts/injector.py` script from the project root to inject the event.

```bash
python scripts/injector.py --url <nats_url> --subject <target_subject> --payload '<json_string>'
```

- `<nats_url>`: NATS connection URL (default: nats://localhost:4222)
- `<target_subject>`: Target NATS subject (e.g. weather.temp)
- `<json_string>`: JSON payload string (e.g. '{"temp": 105}')
