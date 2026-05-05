---
name: knwstack-scaffolder
description: Generates a boilerplate KnwStack application. Use when you need to create a new KnwStack application, tenant, or ruleset.
---

# KnwStack App Scaffolder

## When to use this skill

- Use this when the user asks to create a new KnwStack application.
- Use this to generate boilerplate code for a new tenant or stream.

## How to use it

When triggered, use the `scripts/scaffold.py` script from the project root to generate the application.

```bash
python scripts/scaffold.py --tenant <tenant_name> --streams <stream1> <stream2> --outdir <output_directory>
```

- `<tenant_name>`: The name of the tenant (e.g. weather)
- `<stream1> <stream2>`: Space-separated list of streams (e.g. temp wind)
- `<output_directory>`: Where to place the generated file (default: `.`)
