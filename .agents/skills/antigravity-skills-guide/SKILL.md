---
name: antigravity-skills-guide
description: Guide on how to create, format, and install Antigravity IDE skills. Includes best practices for skill structure, YAML frontmatter requirements, and scope definitions.
---

# Antigravity IDE Skills Guide

## When to use this skill
Use this skill whenever you need to create, update, or manage Antigravity IDE skills within this workspace.

## Overview
Skills are reusable packages of knowledge that extend agent capabilities. A skill is a folder containing a `SKILL.md` file with instructions, best practices, conventions, and optional scripts/resources.

## Scope and Locations
- **Workspace-specific**: `<workspace-root>/.agents/skills/<skill-folder>/`
- **Global (all workspaces)**: `~/.gemini/antigravity/skills/<skill-folder>/`
*(Note: Backward support for `.agent/skills` exists, but `.agents/skills` is the default.)*

## Creating a Skill
1. Create a folder for the skill in the appropriate directory.
2. Add a `SKILL.md` file inside the folder.
3. The `SKILL.md` file MUST contain YAML frontmatter at the top:
   ```yaml
   ---
   name: skill-name-lowercase-hyphens
   description: A clear third-person description of what the skill does and when to use it.
   ---
   ```
4. Below the frontmatter, write the markdown instructions, including sections like `## When to use this skill` and `## How to use it`.

## Skill Folder Structure
```text
<skill-folder>/
├─── SKILL.md       # Main instructions (required)
├─── scripts/       # Helper scripts (optional)
├─── examples/      # Reference implementations (optional)
└─── resources/     # Templates and other assets (optional)
```

## Best Practices
- **Keep skills focused**: Each skill should do one thing well.
- **Write clear descriptions**: Use keywords and third-person phrasing so the agent recognizes when the skill is relevant.
- **Use scripts as black boxes**: If providing scripts, instruct the agent to run them with `--help` instead of reading their source code.
- **Include decision trees**: Help the agent choose the right approach for complex skills.
