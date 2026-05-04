import functools
import inspect
from typing import Callable, Any, Dict, List, Optional

class RuleRegistry:
    """Registry to hold all user-defined rules and prompts."""
    def __init__(self):
        self.reflex_rules = []
        self.tactical_models = []
        self.strategic_prompts = []

    def register_reflex(self, func: Callable, trigger_topic: str):
        self.reflex_rules.append({"func": func, "topic": trigger_topic})

    def register_tactical(self, func: Callable, trigger_topic: str):
        self.tactical_models.append({"func": func, "topic": trigger_topic})

    def register_strategic(self, func: Callable, trigger_topic: str, cooldown_s: int):
        self.strategic_prompts.append({
            "func": func, 
            "topic": trigger_topic,
            "cooldown_s": cooldown_s
        })

# Global registry for the engine to pick up
registry = RuleRegistry()

def reflex_rule(trigger_topic: str):
    """
    Decorator for the Hot Path. 
    Functions decorated with this must be deterministic and execute in <10ms.
    """
    def decorator(func: Callable):
        registry.register_reflex(func, trigger_topic)
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator

def tactical_model(trigger_topic: str):
    """
    Decorator for the Warm Path.
    Functions decorated with this should use fast, local ML models (<100ms).
    """
    def decorator(func: Callable):
        registry.register_tactical(func, trigger_topic)
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator

def strategic_prompt(trigger_topic: str, cooldown_s: int = 60):
    """
    Decorator for the Cold Path.
    Functions decorated with this construct LLM prompts. The engine handles
    the async LiteLLM call and respects the cooldown to avoid flooding.
    """
    def decorator(func: Callable):
        registry.register_strategic(func, trigger_topic, cooldown_s)
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator
