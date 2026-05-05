import bytewax.operators as op
import bytewax.operators.windowing as wop
from bytewax.dataflow import Dataflow
from litellm import completion
import asyncio
import json

from knwstack.connectors.nats import NatsSource, NatsSink
from knwstack.api.decorators import registry
from knwstack.state.windowing import get_cep_window_config
import logging

logger = logging.getLogger(__name__)

def build_engine(nats_url: str = "nats://localhost:4222", input_subject: str = "app.>", output_subject: str = "actions.>"):
    """
    Constructs the core KnwStack Dataflow.
    Reads from NATS, performs multi-tenant routing, and executes the N-Paths.
    """
    flow = Dataflow("knwstack_engine")
    
    # 1. INGESTION
    # Stream is a sequence of (subject, event_dict)
    stream = op.input("nats_in", flow, NatsSource(nats_url, input_subject))

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
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(_execute_strategic_async(prompt_cfg, events, output_subject, nats_url))
                    loop.close()
                
                import threading
                threading.Thread(target=_run_strategic, daemon=True).start()

        return actions_to_publish

    warm_cold_actions = op.flat_map("warm_cold_paths", windowed_stream, execute_warm_cold_paths)

    # 4. MERGE & DISPATCH
    # Combine actions from all paths and publish back to NATS
    all_actions = op.merge("merge_actions", hot_actions, warm_cold_actions)
    op.output("nats_out", all_actions, NatsSink(nats_url))

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
        response = await completion(
            model=messages.get("model", "gpt-3.5-turbo"),
            messages=messages["messages"]
        )
        
        llm_content = response.choices[0].message.content
        
        # Publish the result back to NATS
        import nats
        nc = await nats.connect(nats_url)
        payload = json.dumps({"reasoning": llm_content, "source_events": len(events)}).encode()
        await nc.publish(f"{output_subject}.strategic", payload)
        await nc.close()
        
    except Exception as e:
        logger.error(f"Strategic LLM Error: {e}")
