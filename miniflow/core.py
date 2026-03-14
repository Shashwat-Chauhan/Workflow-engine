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

class Workflow(metaclass=WorkflowMeta):
    def __init__(self , storage : Optional[BaseStorage] = None):
        self.storage = storage or get_storage()
        self._steps : List[StepMeta] = list(self._miniflow_steps)
        self.name = self.__class__.__name__
        self.logger = get_logger(f"workflow.{self.name}")

    def new_run(self) -> RunContext:
        run_id = f"{self.name}-{uuid.uuid4().hex[:8]}"
        ctx = RunContext(run_id=run_id)
        self._persist(ctx)
        return ctx
    
    def _persist(self, ctx: RunContext) -> None:
        payload = {
            "run_id": ctx.run_id,
            "state": ctx.state,
            "step_index": ctx.step_index,
            "status": ctx.status,
            "metadata": ctx.metadata,
        }
        self.storage.save_run(ctx.run_id, payload)
        self.logger.debug("Persisted run %s", ctx.run_id)

    def load_run(self, run_id: str) -> Optional[RunContext]:
        data = self.storage.load_run(run_id)
        if data is None:
            return None
        return RunContext(
            run_id=data["run_id"],
            state=data.get("state", {}),
            step_index=data.get("step_index", 0),
            status=data.get("status", "created"),
            metadata=data.get("metadata", {}),
        )

    def run(self, run_id: Optional[str] = None) -> RunContext:
        if run_id:
            ctx = self.load_run(run_id)
            if ctx is None:
                raise ValueError(f"Unknown run_id {run_id}")
        else:
            ctx = self.new_run()

        ctx.status = "running"
        self._persist(ctx)

        try:
            while ctx.step_index < len(self._steps):
                step_meta = self._steps[ctx.step_index]
                self.logger.info("Running step %s (index=%d)", step_meta.name, ctx.step_index)
                attempt = 0
                while True:
                    try:
                        # call underlying function (bound method)
                        fn = getattr(self, step_meta.name)
                        result = fn(ctx) if self._accepts_ctx(fn) else fn()
                        # store result in state under step name
                        ctx.state[step_meta.name] = result
                        break
                    except Exception as exc:
                        attempt += 1
                        self.logger.exception("Step %s failed on attempt %d: %s", step_meta.name, attempt, exc)
                        if attempt > step_meta.retries:
                            raise
                        backoff = min(2 ** attempt, 30)
                        self.logger.info("Retrying after %s seconds...", backoff)
                        time.sleep(backoff)
                ctx.step_index += 1
                self._persist(ctx)
            ctx.status = "success"
            self._persist(ctx)
            self.logger.info("Run %s completed", ctx.run_id)
        except Exception as exc:
            ctx.status = "failed"
            ctx.metadata["error"] = str(exc)
            self._persist(ctx)
            self.logger.error("Run %s failed: %s", ctx.run_id, exc)
            raise
        return ctx

    @staticmethod
    def _accepts_ctx(fn: Callable[..., Any]) -> bool:
        sig = inspect.signature(fn)
        params = list(sig.parameters.values())
        return len(params) >= 1  # assume first param is ctx
   