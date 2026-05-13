import pathway as pw
import time
import re
import json
import logging
from typing import Dict, Union, List
from functools import partial

from knwstack.api.decorators import registry
from knwstack.connectors.nats_connector import NatsSource

logger = logging.getLogger(__name__)

class InputSchema(pw.Schema):
    subject: str
    data: dict

class KnwStackEngine:
    """Wrapper to hold the Pathway state and allow engine.run()"""
    def run(self):
        """Starts the Pathway engine and begins processing events."""
        logger.info("🚀 KnwStack Engine started. Monitoring event streams...")
        pw.run(monitoring_level=pw.MonitoringLevel.NONE)

# ==========================================
# Logic Helpers (Extracted for Testing)
# ==========================================

import fnmatch

def _match_subject_logic(pattern: str, subject: str) -> bool:
    """Internal matching logic using Regex for precise NATS wildcard simulation."""
    import re
    # Escape dots for regex
    regex_pattern = pattern.replace(".", r"\.")
    # Map NATS wildcards to Regex
    # * -> [^.]+ (one or more non-dot characters)
    # > -> .* (anything)
    regex_pattern = regex_pattern.replace("*", r"[^.]+").replace(">", ".*")
    # Match full string
    return bool(re.fullmatch(regex_pattern, subject))

@pw.udf
def match_subject(pattern: str, subject: str) -> bool:
    """Pathway UDF wrapper for subject matching."""
    return _match_subject_logic(pattern, subject)

def apply_reflex(subject: str, data: dict, reflex_rules: List[dict], output_subject: str) -> dict:
    """Core logic for the Hot Path (Reflex)."""
    # Convert Pathway Json to native dict if necessary
    if not isinstance(data, dict):
        try:
            data = json.loads(str(data))
        except:
            logger.error(f"Failed to parse data for subject {subject}: {data}")
            pass

    logger.debug(f"⚡ [INGEST] {subject} -> {data}")
    
    rule_found = False
    for rule in reflex_rules:
        # Use the plain Python logic here to avoid Pathway Expression errors
        if _match_subject_logic(rule["topic"], subject):
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

@pw.udf
def get_time(data) -> int:
    try:
        if hasattr(data, "get"):
            return int(data.get("timestamp", time.time() * 1000))
        return int(data["timestamp"])
    except:
        return int(time.time() * 1000)

@pw.udf
def get_key(data, subject: str) -> str:
    """Strictly extracts a string key from the 'key' field. No subject fallbacks."""
    val = None
    try:
        # If it's a string/bytes, parse it first
        if isinstance(data, (str, bytes)):
            try: data = json.loads(data)
            except: pass
        
        if isinstance(data, dict):
            val = data.get("key")
        else:
            try: val = data["key"]
            except: pass
    except:
        pass
        
    if val is not None:
        s_val = str(val).strip()
        if s_val and not s_val.startswith("Column("):
            return s_val

    # Log critical warning for missing keys - this event will be lumped into 'unknown'
    # and likely averaged with other key-less events, polluting their state.
    logger.critical(f"🚨 MISSING KEY for event on {subject}! Mandatory 'key' field not found. Data: {data}")
    return "unknown"

def run_tactical(events_list: list, topic: str, model_cfg: dict, output_subject: str) -> dict:
    """Core logic for the Warm Path (Tactical)."""
    if not events_list: return {}
    
    py_events = []
    for item in events_list:
        # Handle both raw data and (subject, data) tuples
        if isinstance(item, tuple) and len(item) == 2:
            s, d = item
        else:
            s, d = topic, item
            
        if not isinstance(d, dict):
            try:
                d = json.loads(str(d))
            except:
                pass
        py_events.append((s, d))
    
    # Identify the partition key for logging
    partition_key = "unknown"
    if py_events:
        _, first_data = py_events[0]
        partition_key = first_data.get("key", topic.split('.')[0])

    logger.info(f"🟠 [WARM] Evaluating Tactical Model '{model_cfg['func'].__name__}' for {partition_key} (Window: {len(py_events)} events)")
    action = model_cfg["func"](py_events)
    if action:
        logger.warning(f"   ∟ [WARM] Outcome: ACTION TRIGGERED -> {action}")
        return {"subject": f"{output_subject}.tactical", "data": action}
    
    logger.debug("   ∟ [WARM] Outcome: No action taken")
    return {}

def run_strategic(events_list: list, topic: str, prompt_cfg: dict, output_subject: str) -> dict:
    """Core logic for the Cold Path (Strategic). Uses synchronous bridge for LLM calls."""
    if not events_list: return {}
    
    py_events = []
    for item in events_list:
        if isinstance(item, tuple) and len(item) == 2:
            s, d = item
        else:
            s, d = topic, item
            
        if not isinstance(d, dict):
            try:
                d = json.loads(str(d))
            except:
                pass
        py_events.append((s, d))
    
    # Identify the partition key for logging
    partition_key = "unknown"
    if py_events:
        _, first_data = py_events[0]
        partition_key = first_data.get("key", topic.split('.')[0])

    logger.info(f"🔵 [COLD] Evaluating Strategic Prompt '{prompt_cfg['func'].__name__}' for {partition_key} (Window: {len(py_events)} events)")
    messages = prompt_cfg["func"](py_events)
    if not messages: 
        logger.debug(f"   ∟ [COLD] Outcome ({partition_key}): No anomalies detected")
        return {}
    
    from litellm import completion
    import asyncio
    try:
        logger.warning(f"   ∟ [COLD] Outcome ({partition_key}): DISPATCHING TO LLM -> {messages.get('model', 'gpt-4o-mini')}")
        # Use synchronous completion as Pathway's apply/reduce are synchronous
        res = completion(model=messages.get("model", "gpt-4o-mini"), messages=messages["messages"])
        content = res.choices[0].message.content
        logger.info(f"✅ [COLD] Strategic Path ({partition_key}): LLM Diagnosis received: {content}")
        return {"subject": f"{output_subject}.strategic", "data": {"reasoning": content, "source_events": len(py_events), "key": partition_key}}
    except Exception as e:
        logger.error(f"❌ [COLD] Strategic LLM Error: {e}")
        return {}

# ==========================================
# Engine Builder
# ==========================================

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
    
    # 1. INGESTION
    t = pw.io.python.read(
        NatsSource(nats_url, subjects, jetstream=bool(jetstream_stream)),
        schema=InputSchema
    )

    # 2. TIME & KEY EXTRACTION
    # We ensure time is in milliseconds and key is always a valid string using typed UDFs
    t = t.with_columns(
        time=get_time(t.data),
        key=get_key(t.data, t.subject)
    )

    # 2. HOT PATH (Reflex)
    reflex_fn = partial(apply_reflex, reflex_rules=registry.reflex_rules, output_subject=output_subject)
    hot_actions = t.select(result=pw.apply(reflex_fn, t.subject, t.data))
    
    def is_valid(r: dict) -> bool:
        return bool(r)

    hot_actions = hot_actions.filter(pw.apply(is_valid, hot_actions.result))
    
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
        # Now using the @pw.udf version of match_subject
        model_table = t.filter(match_subject(topic, t.subject))
        
        if model.get("window_type") == "sliding":
            window = pw.temporal.sliding(duration=model["length_s"] * 1000, hop=model["slide_s"] * 1000)
        else:
            window = pw.temporal.tumbling(duration=model["length_s"] * 1000)

        tactical_fn = partial(run_tactical, topic=topic, model_cfg=model, output_subject=output_subject)
        
        # KEY FIX: Pack subject and data into a tuple column for reduction
        model_table = model_table.with_columns(
            event_tuple=pw.apply(lambda s, d: (s, d), model_table.subject, model_table.data)
        )
        warm_result = model_table.groupby(model_table.key).windowby(model_table.time, window=window).reduce(
            result=pw.apply(tactical_fn, pw.reducers.tuple(pw.this.event_tuple))
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
        # Now using the @pw.udf version
        prompt_table = t.filter(match_subject(topic, t.subject))
        
        if prompt_cfg.get("window_type") == "sliding":
            window = pw.temporal.sliding(duration=prompt_cfg["length_s"] * 1000, hop=prompt_cfg["slide_s"] * 1000)
        else:
            window = pw.temporal.tumbling(duration=prompt_cfg["length_s"] * 1000)

        strategic_fn = partial(run_strategic, topic=topic, prompt_cfg=prompt_cfg, output_subject=output_subject)
        
        # KEY FIX: Pack subject and data into a tuple column for AI analysis
        prompt_table = prompt_table.with_columns(
            event_tuple=pw.apply(lambda s, d: (s, d), prompt_table.subject, prompt_table.data)
        )
        strategic_result = prompt_table.groupby(prompt_table.key).windowby(prompt_table.time, window=window).reduce(
            result=pw.apply(strategic_fn, pw.reducers.tuple(pw.this.event_tuple))
        )
        cold_result = strategic_result.filter(pw.apply(is_valid, strategic_result.result))
        
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
