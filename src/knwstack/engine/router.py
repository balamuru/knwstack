import bytewax.operators as op
import bytewax.operators.window as wop
from bytewax.dataflow import Dataflow
from litellm import completion
import asyncio
import json

from knwstack.connectors.nats import NatsSource, NatsSink
from knwstack.api.decorators import registry
from knwstack.state.windowing import get_cep_window_config

def build_engine(nats_url: str = "nats://localhost:4222", input_subject: str = "app.>", output_subject: str = "actions.>"):
    """
    Constructs the core KnwStack Dataflow.
    Reads from NATS, performs multi-tenant routing, and executes the N-Paths.
    """
    flow = Dataflow("knwstack_engine")
    
    # 1. INGESTION
    # Stream is a sequence of (subject, event_dict)
    stream = op.input("nats_in", flow, NatsSource(nats_url, input_subject))

    # 2. MULTI-TENANT CEP WINDOWING
    # To aggregate cross-stream events (e.g. weather.temp and weather.wind), 
    # we need a grouping key. We use the first part of the subject (the tenant/app ID)
    def extract_tenant_key(msg):
        subject, event_data = msg
        tenant_id = subject.split(".")[0]
        # Bytewax windowing requires (key, value)
        return tenant_id, msg

    keyed_stream = op.map("extract_tenant", stream, extract_tenant_key)
    
    # Apply a Tumbling Window to aggregate events within a timeframe
    clock, window = get_cep_window_config(window_size_seconds=1)
    
    # Collect all events for a tenant within the window into a list
    windowed_stream = wop.collect_window("cep_join", keyed_stream, clock, window)

    # 3. N-PATH ROUTER
    def execute_paths(window_data):
        tenant_id, (window_metadata, events) = window_data
        actions_to_publish = []
        
        # Determine unique subjects present in this window to match rules
        triggered_subjects = set([e[0] for e in events])
        
        # --- PATH 1: REFLEX (HOT) ---
        # Execute sub-10ms deterministic rules
        for rule in registry.reflex_rules:
            if rule["topic"] in triggered_subjects:
                try:
                    # Pass the aggregated events to the rule
                    action = rule["func"](events)
                    if action:
                        actions_to_publish.append((f"{output_subject}.reflex", action))
                except Exception as e:
                    print(f"Reflex Error: {e}")

        # --- PATH 2: TACTICAL (WARM) ---
        # Execute sub-100ms local ML models
        for model in registry.tactical_models:
            if model["topic"] in triggered_subjects:
                try:
                    action = model["func"](events)
                    if action:
                        actions_to_publish.append((f"{output_subject}.tactical", action))
                except Exception as e:
                    print(f"Tactical Error: {e}")

        # --- PATH 3: STRATEGIC (COLD) ---
        # Instead of blocking the dataflow, we dispatch this to a background task
        # Note: In a production Bytewax flow, you'd use a dedicated async operator,
        # but for this reference architecture, asyncio.create_task handles the async 
        # LLM call without blocking the engine's hot path thread.
        for prompt_cfg in registry.strategic_prompts:
            if prompt_cfg["topic"] in triggered_subjects:
                asyncio.create_task(_execute_strategic_async(prompt_cfg, events, output_subject, nats_url))

        return actions_to_publish

    # Route events and flatten the resulting actions
    actions_stream = op.flat_map("router", windowed_stream, execute_paths)

    # 4. ACTION DISPATCH
    op.output("nats_out", actions_stream, NatsSink(nats_url))

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
        print(f"Strategic LLM Error: {e}")
