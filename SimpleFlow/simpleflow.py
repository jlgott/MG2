import asyncio
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union
from pydantic import BaseModel

State = Union[Dict[str, Any], BaseModel]
StepFunc = Callable[[State], Awaitable[State]]

class SimpleFlow:
    def __init__(self):
        self.steps: List[Callable[[State], Awaitable[State]]] = []
        self.conditionals: List[Tuple[Callable[[State], Awaitable[State]], Dict[str, StepFunc], Optional[str]]] = []
        self.parallel_groups: List[List[StepFunc]] = []

    def _prep_state(self, state: State) -> State:
        if isinstance(state, BaseModel):
            return state.model_copy(deep=True)
        elif isinstance(state, dict):
            return state.copy()
        else:
            raise TypeError("Unsupported state type")

    def _post(self, label: str, state: State):
        # Stub for future logging or instrumentation
        pass

    def _merge_states(self, base: State, updates: List[State]) -> State:
        if isinstance(base, BaseModel):
            merged = base.model_copy(deep=True)
            for update in updates:
                merged = merged.model_copy(update=update.model_dump(exclude_unset=True))
            return merged
        elif isinstance(base, dict):
            merged = base.copy()
            for update in updates:
                merged.update(update)
            return merged
        else:
            raise TypeError("Unsupported state type")

    def step(self, fn: StepFunc, retries: int = 0, backoff: float = 0.0, timeout: Optional[float] = None):
        async def wrapped_step(state: State) -> State:
            for attempt in range(retries + 1):
                try:
                    local = self._prep_state(state)
                    result = await asyncio.wait_for(fn(local), timeout) if timeout else await fn(local)
                    self._post(fn.__name__, result)
                    return result
                except Exception:
                    if attempt == retries:
                        raise
                    await asyncio.sleep(backoff)

        self.steps.append(wrapped_step)
        return self

    def parallel(self, fns: List[StepFunc], timeout: Optional[float] = None):
        async def parallel_step(state: State) -> State:
            base = self._prep_state(state)
            coros = []
            for fn in fns:
                local = self._prep_state(base)
                coros.append(asyncio.wait_for(fn(local), timeout) if timeout else fn(local))
            results = await asyncio.gather(*coros)
            merged = self._merge_states(base, results)
            self._post("parallel", merged)
            return merged

        self.steps.append(parallel_step)
        return self

    def if_step(self, fn: StepFunc, branches: Dict[str, StepFunc], key: str, timeout: Optional[float] = None):
        async def conditional_step(state: State) -> State:
            local = self._prep_state(state)
            result = await asyncio.wait_for(fn(local), timeout) if timeout else await fn(local)

            if isinstance(result, BaseModel):
                branch_key = getattr(result, key)
            else:
                branch_key = result[key]

            branch_fn = branches.get(branch_key)
            if not branch_fn:
                raise ValueError(f"No branch handler for key: {branch_key}")

            final = await branch_fn(result)
            self._post(f"if_step:{branch_key}", final)
            return final

        self.steps.append(conditional_step)
        return self

    async def run(self, state: State) -> State:
        current = state
        for step in self.steps:
            current = await step(current)
        return current
