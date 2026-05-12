import pathway as pw
import json
import logging
from typing import Dict, Union

from knwstack.api.decorators import registry
from knwstack.connectors.nats_connector import NatsSource

logger = logging.getLogger(__name__)

class InputSchema(pw.Schema):
    subject: str
    data: dict

class KnwStackEngine:
    """Wrapper to hold the Pathway state and allow engine.run()"""
    def run(self):
        logger.info("Starting Pathway Rust Engine with standard NATS connectors...")
        pw.run()

def build_engine(nats_url: str = "nats://localhost:4222", inputs: Union[str, Dict[str, str]] = "app.>", output_subject: str = "actions.>", jetstream_stream: str = None):
    """
    Constructs the core KnwStack Engine using Pathway.
    """
    if isinstance(inputs, dict):
        subjects = list(inputs.keys())
    elif isinstance(inputs, list):
        subjects = inputs
    else:
        subjects = [inputs]
    
    def parse_payload(payload: bytes) -> dict:
        import json
        try:
            return json.loads(payload.decode())
        except:
            return {}

    # 1. INGESTION
    t = pw.io.python.read(
        NatsSource(nats_url, subjects, jetstream=bool(jetstream_stream)),
        schema=InputSchema
    )

    # 2. TIME & WINDOWING
    t = t.with_columns(time=t.data["timestamp"].as_int(default=0))

    # 2. HOT PATH (Reflex)
    def apply_reflex(subject: str, data: dict) -> dict:
        # Convert Pathway Json to native dict if necessary
        if not isinstance(data, dict):
            import json
            try:
                data = json.loads(str(data))
            except:
                logger.error(f"Failed to parse data for subject {subject}: {data}")
                pass

        logger.debug(f"⚡ [INGEST] {subject} -> {data}")
        
        rule_found = False
        for rule in registry.reflex_rules:
            if rule["topic"] == subject:
                rule_found = True
                logger.info(f"   ∟ [HOT] Matching Rule: {rule['func'].__name__}")
                action = rule["func"]([(subject, data)])
                if action:
                    logger.warning(f"   ∟ [HOT] Outcome: ACTION TRIGGERED -> {action}")
                    return {"subject": f"{output_subject}.reflex", "data": action}
        
        if rule_found:
            logger.debug("   ∟ [HOT] Outcome: No action taken (logic conditions not met)")
        else:
            logger.debug(f"   ∟ [HOT] Outcome: No matching reflex rules for {subject}")
            
        return {}

    hot_actions = t.select(result=pw.apply(apply_reflex, t.subject, t.data))
    def is_valid(r: dict) -> bool:
        return bool(r)

    hot_actions = hot_actions.filter(pw.apply(is_valid, hot_actions.result))
    
    # WRITE (Using standard Pathway NATS connector)
    pw.io.nats.write(
        hot_actions.select(
            subject=hot_actions.result["subject"],
            data=hot_actions.result["data"]
        ),
        nats_url,
        topic=f"{output_subject}.reflex",
        format="json"
    )

    # 3. WARM PATH (Tactical)
    for model in registry.tactical_models:
        topic = model["topic"]
        model_table = t.filter(t.subject == topic)
        
        # Configure Window
        if model.get("window_type") == "sliding":
            window = pw.temporal.sliding(duration=model["length_s"] * 1000, hop=model["slide_s"] * 1000)
        else:
            window = pw.temporal.tumbling(duration=model["length_s"] * 1000)

        def run_tactical(events_list: list) -> dict:
            if not events_list: return {}
            
            # Convert Pathway Json objects to native dicts
            import json
            py_events = []
            for d in events_list:
                if not isinstance(d, dict):
                    try:
                        d = json.loads(str(d))
                    except:
                        pass
                py_events.append((topic, d))
            
            logger.info(f"🟠 [WARM] Evaluating Tactical Model '{model['func'].__name__}' for {topic} (Window: {len(py_events)} events)")
            action = model["func"](py_events)
            if action:
                logger.warning(f"   ∟ [WARM] Outcome: ACTION TRIGGERED -> {action}")
                return {"subject": f"{output_subject}.tactical", "data": action}
            
            logger.debug("   ∟ [WARM] Outcome: No action taken")
            return {}

        warm_result = model_table.windowby(model_table.time, window=window).reduce(
            result=pw.apply(run_tactical, pw.reducers.tuple(pw.this.data))
        )
        warm_result = warm_result.filter(pw.apply(is_valid, warm_result.result))
        
        pw.io.nats.write(
            warm_result.select(
                subject=warm_result.result["subject"],
                data=warm_result.result["data"]
            ),
            nats_url,
            topic=f"{output_subject}.tactical",
            format="json"
        )

    # 4. COLD PATH (Strategic)
    for prompt_cfg in registry.strategic_prompts:
        topic = prompt_cfg["topic"]
        prompt_table = t.filter(t.subject == topic)
        
        if prompt_cfg.get("window_type") == "sliding":
            window = pw.temporal.sliding(duration=prompt_cfg["length_s"] * 1000, hop=prompt_cfg["slide_s"] * 1000)
        else:
            window = pw.temporal.tumbling(duration=prompt_cfg["length_s"] * 1000)

        # Pathway native Async UDFs
        async def run_strategic(events_list: list) -> dict:
            if not events_list: return {}
            
            # Convert Pathway Json objects to native dicts
            import json
            py_events = []
            for d in events_list:
                if not isinstance(d, dict):
                    try:
                        d = json.loads(str(d))
                    except:
                        pass
                py_events.append((topic, d))
            
            logger.info(f"🔵 [COLD] Evaluating Strategic Prompt '{prompt_cfg['func'].__name__}' for {topic} (Window: {len(py_events)} events)")
            messages = prompt_cfg["func"](py_events)
            if not messages: 
                logger.debug("   ∟ [COLD] Outcome: No anomalies detected")
                return {}
            
            from litellm import acompletion
            try:
                logger.warning(f"   ∟ [COLD] Outcome: DISPATCHING TO LLM -> {messages.get('model', 'gpt-4o-mini')}")
                res = await acompletion(model=messages.get("model", "gpt-4o-mini"), messages=messages["messages"])
                content = res.choices[0].message.content
                logger.info(f"✅ [COLD] Strategic Path: LLM Diagnosis received: {content}")
                return {"subject": f"{output_subject}.strategic", "data": {"reasoning": content, "source_events": len(py_events)}}
            except Exception as e:
                logger.error(f"❌ [COLD] Strategic LLM Error: {e}")
                return {}

        cold_result = prompt_table.windowby(prompt_table.time, window=window).reduce(
            result=pw.apply(run_strategic, pw.reducers.tuple(pw.this.data))
        )
        cold_result = cold_result.filter(pw.apply(is_valid, cold_result.result))
        
        pw.io.nats.write(
            cold_result.select(
                subject=cold_result.result["subject"],
                data=cold_result.result["data"]
            ),
            nats_url,
            topic=f"{output_subject}.strategic",
            format="json"
        )

    return KnwStackEngine()
