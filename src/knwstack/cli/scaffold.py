import argparse
import os
import shutil
import sys
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description="KnwStack Scaffolding Tool")
    parser.add_argument("name", help="Name of the new KnwStack application")
    parser.add_argument("--template", default="basic", help="Template to use (currently only 'basic' is supported)")
    
    args = parser.parse_args()
    
    project_dir = Path.cwd() / args.name
    if project_dir.exists():
        print(f"Error: Directory '{args.name}' already exists.")
        sys.exit(1)
        
    print(f"Scaffolding KnwStack application '{args.name}'...")
    
    # Create directory structure
    os.makedirs(project_dir)
    
    # Create a basic app.py
    app_py_content = f"""import logging
from knwstack.api.decorators import reflex_rule, tactical_model, strategic_prompt
from knwstack.engine.router import build_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- HOT PATH: Reflex Rule ---
@reflex_rule("{args.name}.alarm")
def emergency_reflex(events):
    # Triggered instantly when an alarm event is received
    for topic, data in events:
        logger.warning(f"🚨 EMERGENCY DETECTED on {{topic}}! Taking reflex action.")
    return {{"status": "emergency_handled"}}

# --- WARM PATH: Tactical Model ---
@tactical_model("{args.name}.telemetry")
def telemetry_tactical(events):
    # Processes the 1-second aggregate window
    logger.info(f"📊 Processing telemetry window with {{len(events)}} events.")
    return None

# --- COLD PATH: Strategic Prompt ---
@strategic_prompt("{args.name}.telemetry", cooldown_s=300)
def analysis_strategic(events):
    # Dispatched to LLM asynchronously
    return {{
        "model": "gpt-4o-mini",
        "messages": [
            {{"role": "user", "content": f"Analyze this telemetry: {{events}}"}}
        ]
    }}

# Initialize the engine
flow = build_engine(
    nats_url="nats://localhost:4222",
    input_subject="{args.name}.>",
    output_subject="{args.name}.actions"
)
"""
    
    with open(project_dir / "app.py", "w") as f:
        f.write(app_py_content)
        
    # Create a basic README
    readme_content = f"""# {args.name}
    
A KnwStack Real-Time AI Application.

## Getting Started

1. Ensure NATS is running.
2. Run the application:
   ```bash
   uv run python -m bytewax.run app:flow
   ```
"""
    with open(project_dir / "README.md", "w") as f:
        f.write(readme_content)
        
    print(f"Successfully created '{args.name}' at {project_dir}")
    print("\nNext steps:")
    print(f"  cd {args.name}")
    print("  uv run python -m bytewax.run app:flow")

if __name__ == "__main__":
    main()
