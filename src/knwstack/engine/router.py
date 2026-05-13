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
# Stateful tracking for cooldowns: {(model_name, key): last_execution_time}
_last_tactical_run = {}

class InputSchema(pw.Schema):
    subject: str
    data: dict
    time: int

class KnwStackEngine:
    """Defines the KnwStack Dataflow Graph logic."""
    def __init__(self, nats_url: str = "nats://localhost:4222", inputs: Union[str, Dict[str, str]] = "app.>", output_subject: str = "actions.>", jetstream_stream: str = None):
        self.nats_url = nats_url
        self.inputs = inputs
        self.output_subject = output_subject
        self.jetstream_stream = jetstream_stream

    def build(self):
        """Constructs the Pathway dataflow graph. Does NOT execute."""
        logger.info(f"🏗️  Building KnwStack Dataflow Graph for {self.inputs}...")
        
        if isinstance(self.inputs, dict):
            subjects = list(self.inputs.keys())
        elif isinstance(self.inputs, list):
            subjects = self.inputs
        else:
            subjects = [self.inputs]

        # 1. INGESTION
        t = pw.io.python.read(
            NatsSource(self.nats_url, subjects, jetstream=bool(self.jetstream_stream)),
            schema=InputSchema
        )

        # 2. KEY EXTRACTION
        t = t.with_columns(
            key=pw.apply(get_key, pw.this.data, pw.this.subject)
        )

        # 3. HOT PATH (Reflex)
        reflex_fn = partial(apply_reflex, reflex_rules=registry.reflex_rules, output_subject=self.output_subject)
        hot_actions = t.select(result=pw.apply(reflex_fn, t.subject, t.data))
        
        def is_valid(r: dict) -> bool:
            return bool(r)

        hot_actions = hot_actions.filter(pw.apply(is_valid, hot_actions.result))
        
        pw.io.nats.write(
            hot_actions.select(
                subject=hot_actions.result["subject"],
                data=hot_actions.result["data"]
            ),
            self.nats_url,
            topic=f"{self.output_subject}.reflex",
            format="json"
        )

        # 4. WARM PATH (Tactical)
        for model in registry.tactical_models:
            topic = model["topic"]
            model_table = t.filter(pw.apply(match_subject, topic, t.subject))
            
            if model.get("window_type") == "sliding":
                window = pw.temporal.sliding(duration=model["length_s"] * 1000, hop=model["slide_s"] * 1000)
            else:
                window = pw.temporal.tumbling(duration=model["length_s"] * 1000)

            tactical_fn = partial(run_tactical, topic=topic, model_cfg=model, output_subject=self.output_subject)
            
            model_table = model_table.with_columns(
                event_tuple=pw.apply(lambda s, d: (s, d), model_table.subject, model_table.data)
            )
            warm_result = model_table.groupby(pw.this.key).windowby(pw.this.time, window=window).reduce(
                result=pw.apply(tactical_fn, pw.reducers.tuple(pw.this.event_tuple))
            )
            warm_result = warm_result.filter(pw.apply(is_valid, warm_result.result))
            
            pw.io.nats.write(
                warm_result.select(
                    subject=warm_result.result["subject"],
                    data=warm_result.result["data"]
                ),
                self.nats_url,
                topic=f"{self.output_subject}.tactical",
                format="json"
            )

        # 5. COLD PATH (Strategic)
        for prompt_cfg in registry.strategic_prompts:
            topic = prompt_cfg["topic"]
            prompt_table = t.filter(pw.apply(match_subject, topic, t.subject))
            
            if prompt_cfg.get("window_type") == "sliding":
                window = pw.temporal.sliding(duration=prompt_cfg["length_s"] * 1000, hop=prompt_cfg["slide_s"] * 1000)
            else:
                window = pw.temporal.tumbling(duration=prompt_cfg["length_s"] * 1000)

            strategic_fn = partial(run_strategic, topic=topic, prompt_cfg=prompt_cfg, output_subject=self.output_subject)
            
            prompt_table = prompt_table.with_columns(
                event_tuple=pw.apply(lambda s, d: (s, d), prompt_table.subject, prompt_table.data)
            )
            strategic_result = prompt_table.groupby(pw.this.key).windowby(pw.this.time, window=window).reduce(
                result=pw.apply(strategic_fn, pw.reducers.tuple(pw.this.event_tuple))
            )
            cold_result = strategic_result.filter(pw.apply(is_valid, strategic_result.result))
            
            pw.io.nats.write(
                cold_result.select(
                    subject=cold_result.result["subject"],
                    data=cold_result.result["data"]
                ),
                self.nats_url,
                topic=f"{self.output_subject}.strategic",
                format="json"
            )

class KnwStackRunner:
    """Handles the execution lifecycle of the KnwStack Engine."""
    def __init__(self, engine: KnwStackEngine):
        self.engine = engine

    def run(self, dashboard: bool = False, stats: bool = False, port: int = 9090):
        """Executes the engine with optional monitoring.
        
        Args:
            dashboard: Enable the Prometheus metrics server (Web).
            stats: Enable the classic terminal UI (Stats).
            port: Port for the metrics server.
        """
        # 1. Build the dataflow graph
        self.engine.build()

        # 2. Configure execution mode
        if stats:
            # CLASSIC MODE: Show terminal UI, no web server
            logger.info("📊 KnwStack Engine started in STATS mode (Terminal UI enabled).")
            pw.run(monitoring_level=pw.MonitoringLevel.ALL, with_http_server=False)
            
        elif dashboard:
            # DASHBOARD MODE: Web metrics server, silent terminal
            import os
            from contextlib import contextmanager
            import pathway.internals.monitoring
            
            logger.info(f"📊 KnwStack Engine started in DASHBOARD mode (Web server at http://localhost:{port}/metrics).")
            
            # SILENCE PATCH: Suppress the terminal dashboard
            @contextmanager
            def noop_live(*args, **kwargs): yield
            pathway.internals.monitoring.Live = noop_live

            # Use environment variables for max compatibility
            os.environ["PATHWAY_WEBSERVER_PORT"] = str(port)
            os.environ["PATHWAY_MONITORING_HTTP_PORT"] = str(port)
            pw.run(monitoring_level=pw.MonitoringLevel.ALL, with_http_server=True)
            
        else:
            # HEADLESS MODE: Silent terminal, no web server
            logger.info("🚀 KnwStack Engine started in HEADLESS mode (Production).")
            pw.run(monitoring_level=pw.MonitoringLevel.NONE, with_http_server=False)

# ==========================================
# Logic Helpers (Extracted for Testing)
# ==========================================

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

def match_subject(pattern: str, subject: str) -> bool:
    """Pathway UDF wrapper for subject matching."""
    return _match_subject_logic(pattern, subject)

def apply_reflex(subject: str, data: dict, reflex_rules: List[dict], output_subject: str) -> dict:
    """Core logic for the Hot Path (Reflex)."""
    # Convert Pathway Json to native dict if necessary
    if not isinstance(data, dict):
        try:
            import json
            data = json.loads(str(data))
        except:
            logger.error(f"Failed to parse data for subject {subject}: {data}")
            pass

    logger.debug(f"⚡ [INGEST] {subject} -> {data}")
    
    rule_found = False
    for rule in reflex_rules:
        if _match_subject_logic(rule["topic"], subject):
            rule_found = True
            logger.info(f"   ∟ [HOT] Matching Rule: {rule['func'].__name__}")
            action = rule["func"]([(subject, data)])
            if action:
                logger.warning(f"   ∟ [HOT] Outcome: ACTION TRIGGERED -> {action}")
                return {"subject": f"{output_subject}.reflex", "data": action}
    return {}

def get_key(data, subject: str) -> str:
    """Strictly extracts a string key from the 'key' field."""
    val = None
    try:
        if hasattr(data, "get"): val = data.get("key")
        elif isinstance(data, dict): val = data.get("key")
        if val is None: val = data["key"]
    except: pass
    
    if val is not None: return str(val).strip()
    logger.critical(f"🚨 MISSING KEY for event on {subject}! Data: {data}")
    return "unknown"

def run_tactical(events_list: list, topic: str, model_cfg: dict, output_subject: str) -> dict:
    """Core logic for the Warm Path (Tactical)."""
    if not events_list: return {}
    py_events = []
    for item in events_list:
        if isinstance(item, tuple) and len(item) == 2: s, d = item
        else: s, d = topic, item
        if not isinstance(d, dict):
            try: 
                import json
                d = json.loads(str(d))
            except: pass
        py_events.append((s, d))

    partition_key = py_events[0][1].get("key", "unknown") if py_events else "unknown"
    if partition_key != "unknown":
        py_events = [e for e in py_events if e[1].get("key") == partition_key]
    
    now = time.time()
    cooldown = model_cfg.get("cooldown_s", 0)
    cooldown_key = (model_cfg["func"].__name__, partition_key)
    
    if cooldown > 0:
        last_run = _last_tactical_run.get(cooldown_key, 0)
        if now - last_run < cooldown: return {}

    logger.info(f"🟠 [WARM] Evaluating Tactical Model '{model_cfg['func'].__name__}' for {partition_key}")
    action = model_cfg["func"](py_events)
    if action:
        if cooldown > 0: _last_tactical_run[cooldown_key] = now
        return {"subject": f"{output_subject}.tactical", "data": action}
    return {}

def run_strategic(events_list: list, topic: str, prompt_cfg: dict, output_subject: str) -> dict:
    """Core logic for the Cold Path (Strategic)."""
    if not events_list: return {}
    py_events = []
    for item in events_list:
        if isinstance(item, tuple) and len(item) == 2: s, d = item
        else: s, d = topic, item
        if not isinstance(d, dict):
            try:
                import json
                d = json.loads(str(d))
            except: pass
        py_events.append((s, d))
    
    partition_key = py_events[0][1].get("key", "unknown") if py_events else "unknown"
    logger.info(f"🔵 [COLD] Evaluating Strategic Prompt for {partition_key}")
    messages = prompt_cfg["func"](py_events)
    if not messages: return {}
    
    from litellm import completion
    try:
        res = completion(model=messages.get("model", "gpt-4o-mini"), messages=messages["messages"])
        content = res.choices[0].message.content
        return {"subject": f"{output_subject}.strategic", "data": {"reasoning": content, "key": partition_key}}
    except Exception as e:
        logger.error(f"❌ [COLD] Strategic LLM Error: {e}")
        return {}
