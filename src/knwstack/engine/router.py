import bytewax.operators as op
import bytewax.operators.windowing as wop
from bytewax.dataflow import Dataflow
from litellm import completion
import asyncio
import json

from knwstack.connectors.nats import NatsSource, NatsCoreSource, NatsSink
from knwstack.api.decorators import registry
from knwstack.state.windowing import get_cep_window_config
import logging

logger = logging.getLogger(__name__)

from typing import Dict, Union

def build_engine(
    nats_url: str = "nats://localhost:4222", 
    inputs: Union[str, Dict[str, str]] = "app.>", 
    output_subject: str = "actions.>"
):
    """
    Constructs the core KnwStack Dataflow.
    Supports a single subject string or a dict mapping {subject: mode}.
    """
    flow = Dataflow("knwstack_engine")
    
    # 1. INGESTION
    # Normalize inputs to a dict: {subject: mode}
    if isinstance(inputs, str):
        input_map = {inputs: "reliable"}
    else:
        input_map = inputs

    input_streams = []
    for subject, mode in input_map.items():
        if mode == "superhot":
            logger.info(f"🚀 Ingesting '{subject}' in SUPERHOT mode (NATS Core Push)")
            source = NatsCoreSource(nats_url, subject)
        else:
            logger.info(f"🛡️ Ingesting '{subject}' in RELIABLE mode (NATS JetStream Pull)")
            source = NatsSource(nats_url, subject)
        
        safe_id = subject.replace(".", "_").replace(">", "all")
        input_streams.append(op.input(f"nats_in_{safe_id}", flow, source))

    # Merge all input streams into a single processing stream
    if len(input_streams) > 1:
        stream = op.merge("merge_inputs", *input_streams)
    else:
        stream = input_streams[0]

    # 2. HOT PATH (REFLEX)
    # Execute deterministic rules IMMEDIATELY without waiting for a window.
    def execute_hot_path(msg):
        subject, event_data = msg
        if subject.startswith("knwstack.internal."):
            return []
            
        logger.debug(f"Hot Path: Evaluating rules for subject '{subject}'")
        actions = []
        for rule in registry.reflex_rules:
            if rule["topic"] == subject:
                try:
                    # Pass as a list of one event for API consistency
                    action = rule["func"]([msg])
                    if action:
                        actions.append((f"{output_subject}.reflex", action))
                except Exception as e:
                    logger.error(f"Reflex Error: {e}")
        return actions

    hot_actions = op.flat_map("hot_path", stream, execute_hot_path)

    # 3. WARM/COLD PATHS (WINDOWED)
    # To aggregate cross-stream events, we key by tenant ID.
    def extract_tenant_key(msg):
        subject, event_data = msg
        tenant_id = subject.split(".")[0]
        return tenant_id, msg

    keyed_stream = op.map("extract_tenant", stream, extract_tenant_key)
    clock, window = get_cep_window_config(window_size_seconds=1)
    window_out = wop.collect_window("cep_join", keyed_stream, clock, window)
    windowed_stream = window_out.down

    def execute_warm_cold_paths(window_data):
        tenant_id, (window_metadata, events) = window_data
        if tenant_id != "knwstack":
            logger.info(f"Engine: Processing window for tenant '{tenant_id}' with {len(events)} events.")
        
        actions_to_publish = []
        triggered_subjects = set([e[0] for e in events])
        
        # --- PATH 2: TACTICAL (WARM) ---
        for model in registry.tactical_models:
            if model["topic"] in triggered_subjects:
                try:
                    action = model["func"](events)
                    if action:
                        actions_to_publish.append((f"{output_subject}.tactical", action))
                except Exception as e:
                    logger.error(f"Tactical Error: {e}")

        # --- PATH 3: STRATEGIC (COLD) ---
        for prompt_cfg in registry.strategic_prompts:
            if prompt_cfg["topic"] in triggered_subjects:
                def _run_strategic():
                    try:
                        asyncio.run(_execute_strategic_async(prompt_cfg, events, output_subject, nats_url))
                    except Exception as e:
                        logger.error(f"Strategic Thread Error: {e}")
                
                import threading
                threading.Thread(target=_run_strategic, daemon=True).start()

        return actions_to_publish

    warm_cold_actions = op.flat_map("warm_cold_paths", windowed_stream, execute_warm_cold_paths)

    # 4. DISPATCH
    # We output Hot Path and Warm Path independently so the fast Hot Path 
    # is NEVER blocked by the windowing watermark of the Warm Path.
    op.output("nats_out_hot", hot_actions, NatsSink(nats_url))
    op.output("nats_out_warm", warm_cold_actions, NatsSink(nats_url))

    return flow

async def _execute_strategic_async(prompt_cfg, events, output_subject, nats_url):
    """Executes the LLM prompt asynchronously to prevent blocking the hot path."""
    try:
        # Construct the prompt using the user's registered function
        messages = prompt_cfg["func"](events)
        
        if not messages:
            return

        # Call the LLM using LiteLLM (automatically handles OpenAI, Anthropic, etc.)
        # The specific model is configured by the user in the prompt function
        # For default, we assume the user returns a valid LiteLLM messages payload
        from litellm import acompletion
        response = await acompletion(
            model=messages.get("model", "gpt-3.5-turbo"),
            messages=messages["messages"]
        )
        
        llm_content = response.choices[0].message.content
        logger.info(f"✅ Strategic Path: LLM Diagnosis received: {llm_content}")
        
        # Publish the result back to NATS
        import nats
        nc = await nats.connect(nats_url)
        payload = json.dumps({"reasoning": llm_content, "source_events": len(events)}).encode()
        await nc.publish(f"{output_subject}.strategic", payload)
        await nc.close()
        
    except Exception as e:
        logger.error(f"Strategic LLM Error: {e}")
