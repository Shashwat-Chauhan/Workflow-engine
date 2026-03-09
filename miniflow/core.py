from __future__ import annotations
import inspect
import uuid
import time
from dataclasses import dataclass , field
from typing import Any , Callable , Dict , List , Optional
from .storage import BaseStorage , get_storage
from .logger import get_logger

logger = get_logger("miniflow.core")


@dataclass
class StepMeta:
    name: str
    fn : Callable[..., Any]
    retries : int = 0
    order : int = 0


def Step(order: int = 0 , retries : int = 0):
    def decorator(fn):
        fn.__miniflow_step__ = StepMeta(
            name = fn.__name__,
            fn = fn,
            reties = retries,
            order = order
        )
        return fn
    return decorator


@dataclass
class RunContext:
    run_id: str
    state: Dict[str, Any] = field(default_factory=dict)
    step_index: int = 0
    status: str = "created"  # running, success, failed
    metadata: Dict[str, Any] = field(default_factory=dict)


class WorkflowMeta(type):
    def __new__(mcls, name, bases, attrs):
        steps: List[StepMeta] = []
        for key, val in list(attrs.items()):
            if hasattr(val, "__miniflow_step__"):
                meta: StepMeta = getattr(val, "__miniflow_step__")
                steps.append(meta)
        steps.sort(key=lambda s: s.order)
        attrs["_miniflow_steps"] = steps
        return super().__new__(mcls, name, bases, attrs)
