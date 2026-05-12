#!/usr/bin/env python3
"""
<name>KnwStack App Scaffolder</name>
<description>
Automates the creation of boilerplate KnwStack applications. 
It generates an `app.py` file with the necessary KnwStack Developer API imports 
and placeholder `@reflex_rule`, `@tactical_model`, and `@strategic_prompt` decorators 
for the requested streams.
</description>
<trigger>When the user asks to create a new KnwStack application, tenant, or ruleset.</trigger>
"""
import argparse
import os

TEMPLATE = """import logging
from knwstack.api.decorators import reflex_rule, tactical_model, strategic_prompt
from knwstack.engine.router import build_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ==========================================
# KnwStack Tenant Application: {tenant_name}
# ==========================================

{reflexes}

{tacticals}

{strategics}

# ==========================================
# Engine Initialization
# ==========================================
engine = build_engine(
    nats_url="nats://localhost:4222", 
    inputs={subjects},
    output_subject="{tenant_name_lower}.actions"
)

if __name__ == "__main__":
    try:
        engine.run()
    except KeyboardInterrupt:
        logger.info("Shutting down KnwStack Engine...")
"""

REFLEX_TEMPLATE = """@reflex_rule("{tenant_name}.{stream}")
def {stream_slug}_reflex(events):
    \"\"\"Hot Path: Sub-10ms deterministic execution.\"\"\"
    for topic, data in events:
        if topic == "{tenant_name}.{stream}":
            # TODO: Implement fast physical reflex logic
            pass
    return None
"""

TACTICAL_TEMPLATE = """@tactical_model("{tenant_name}.{stream}")
def {stream_slug}_tactical(events):
    \"\"\"Warm Path: Sub-100ms ML classification.\"\"\"
    for topic, data in events:
        if topic == "{tenant_name}.{stream}":
            # TODO: Implement fast local ML inference
            pass
    return None
"""

STRATEGIC_TEMPLATE = """@strategic_prompt("{tenant_name}.{stream}", cooldown_s=60)
def {stream_slug}_strategic(events):
    \"\"\"Cold Path: Asynchronous LLM reasoning via LiteLLM.\"\"\"
    messages = []
    for topic, data in events:
        if topic == "{tenant_name}.{stream}":
            # TODO: Construct prompt based on anomalies
            messages.append({{
                "role": "user",
                "content": f"Analyze this event: {{data}}"
            }})
            
    if not messages:
        return None
        
    return {{
        "model": "gpt-4o",
        "messages": messages
    }}
"""

def generate_scaffold(tenant_name: str, streams: list, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, f"{tenant_name}_app.py")
    
    reflexes = []
    tacticals = []
    strategics = []
    
    for stream in streams:
        stream_slug = stream.replace(".", "_").replace("-", "_")
        ctx = {"tenant_name": tenant_name, "stream": stream, "stream_slug": stream_slug}
        
        reflexes.append(REFLEX_TEMPLATE.format(**ctx))
        tacticals.append(TACTICAL_TEMPLATE.format(**ctx))
        strategics.append(STRATEGIC_TEMPLATE.format(**ctx))
        
    full_subjects = [f"{tenant_name}.{s}" for s in streams]
    
    content = TEMPLATE.format(
        tenant_name=tenant_name.upper(),
        tenant_name_lower=tenant_name.lower(),
        subjects=full_subjects,
        reflexes="\n".join(reflexes),
        tacticals="\n".join(tacticals),
        strategics="\n".join(strategics)
    )
    
    with open(file_path, "w") as f:
        f.write(content)
        
    print(f"✅ Successfully scaffolded KnwStack app at: {file_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scaffold a KnwStack App.")
    parser.add_argument("--tenant", required=True, help="Name of the tenant (e.g. weather)")
    parser.add_argument("--streams", nargs="+", required=True, help="List of streams (e.g. temp wind)")
    parser.add_argument("--outdir", default=".", help="Output directory")
    args = parser.parse_args()
    
    generate_scaffold(args.tenant, args.streams, args.outdir)
