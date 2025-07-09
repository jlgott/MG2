import asyncio
import os
import socket
import time  # noqa: F401
from copy import deepcopy # noqa: F401
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

from pydantic import BaseModel

# --- Define the State ---
State = Union[Dict[str, Any], BaseModel]
StepResult = Tuple[State, Optional[str]]
FlowFunc = Callable[[State], Awaitable[StepResult]]


# --- Flow Controller ---
class FunctionalFlow:
    def __init__(
        self,
        *,
        default_retries: int = 1,
        default_backoff: float = 0.0,
        default_timeout: Optional[float] = None,
        enable_post: bool = True,
        collector: Optional[Callable[[Tuple], None]] = None,
    ):
        self.steps: Dict[str, FlowFunc] = {}
        self.start: str = ""
        self.state: Optional[State] = None
        self.enable_post = enable_post
        self.collector = collector
        self.default_retries = default_retries
        self.default_backoff = default_backoff
        self.default_timeout = default_timeout
        self.custom_merge: Optional[Callable[[List[State]], State]] = None
        self.user_account = os.getlogin()
        self.machine = socket.gethostname()

    def set_default_timeout(self, timeout: float):
        self.default_timeout = timeout

    def _get_func_name(self, func: Union[str, Callable, None]) -> Optional[str]:
        if callable(func):
            return func.__name__
        if isinstance(func, str):
            return func
        return None

    def initial_state(self, state: State):
        self.state = state

    def _copy_state(self, state: State) -> State:
        if isinstance(state, BaseModel):
            return state.model_copy(deep=True)
        return deepcopy(state)

    def _merge_states(self, states: List[Any]) -> State:
        # Unwrap (state, next_step) tuples if needed
        if isinstance(states[0], tuple):
            states = [s[0] for s in states]

        # Use custom merge if provided
        if self.custom_merge:
            return self.custom_merge(states)

        # Merge Pydantic models
        if isinstance(states[0], BaseModel):
            base = states[0].model_copy(deep=True)
            for s in states[1:]:
                base = base.model_copy(update=s.model_dump(exclude_unset=True))
            return base

        # Merge dicts with collision safety
        if isinstance(states[0], dict):
            merged = {}
            for s in states:
                for k, v in s.items():
                    if k in merged and merged[k] != v:
                        raise ValueError(f"Conflict on key '{k}': {merged[k]} != {v}")
                    merged[k] = v
            return merged

        raise TypeError("Unsupported state type for merging")

    def step(
        self,
        func: FlowFunc,
        next_step: Union[str, Callable] = None,
        retries: int = None,
        backoff: float = None,
        timeout: Optional[float] = None,
        ignore_timeout: bool = False,
    ):
        name = self._get_func_name(func)
        next_step_name = self._get_func_name(next_step)
        retries = retries if retries is not None else self.default_retries
        backoff = backoff if backoff is not None else self.default_backoff
        timeout = timeout if timeout is not None else self.default_timeout

        async def wrapper(state: State) -> StepResult:
            for attempt in range(retries):
                try:
                    if timeout:
                        new_state, _ = await asyncio.wait_for(
                            func(state), timeout=timeout
                        )
                    else:
                        new_state, _ = await func(state)
                    return new_state, next_step_name
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    if ignore_timeout:
                        return state, next_step_name
                    if attempt == retries - 1:
                        raise
                except Exception as e:
                    if attempt == retries - 1:
                        raise e
                    await asyncio.sleep(backoff)

        wrapper.__name__ = name
        self.steps[name] = wrapper
        if not self.start:
            self.start = name

    def if_step(
        self,
        func: FlowFunc,
        cases: Dict[str, Union[str, Callable]],
        retries: int = None,
        backoff: float = None,
        timeout: Optional[float] = None,
        ignore_timeout: bool = False,
    ):
        name = self._get_func_name(func)
        case_map = {k: self._get_func_name(v) for k, v in cases.items()}
        retries = retries if retries is not None else self.default_retries
        backoff = backoff if backoff is not None else self.default_backoff
        timeout = timeout if timeout is not None else self.default_timeout

        async def wrapper(state: State) -> StepResult:
            for attempt in range(retries):
                try:
                    if timeout:
                        new_state, branch = await asyncio.wait_for(
                            func(state), timeout=timeout
                        )
                    else:
                        new_state, branch = await func(state)
                    next_step = case_map.get(branch)
                    return new_state, next_step
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    if ignore_timeout:
                        return state, None
                    if attempt == retries - 1:
                        raise
                except Exception as e:
                    if attempt == retries - 1:
                        raise e
                    await asyncio.sleep(backoff)

        wrapper.__name__ = name
        self.steps[name] = wrapper
        if not self.start:
            self.start = name

    def parallel(
        self,
        name: str,
        funcs: List[Callable[[State], Awaitable[State]]],
        merge: Optional[Callable[[List[State]], State]] = None,
        next_step: Union[str, Callable] = None,
        timeout: Optional[float] = None,
        ignore_timeout: bool = False,
    ):
        next_step_name = self._get_func_name(next_step)
        timeout = timeout if timeout is not None else self.default_timeout
        if merge:
            self.custom_merge = merge

        async def tracked_func(fn, label, input_state):
            try:
                if timeout:
                    result = await asyncio.wait_for(fn(input_state), timeout=timeout)
                    if self.collector:
                        self.collector((label, result, name, self.user_account, self.machine))
                    return result
                else:
                    result = await fn(input_state)
                    if self.collector:
                        self.collector((label, result, name, self.user_account, self.machine))
                    return result
            except (asyncio.TimeoutError, asyncio.CancelledError):
                if ignore_timeout:
                    if self.collector:
                        self.collector((label + ":timeout_skipped", input_state, name, self.user_account, self.machine))
                    return input_state
                raise

        async def parallel_step(state: State) -> StepResult:
            results = await asyncio.gather(
                *(
                    tracked_func(f, self._get_func_name(f), self._copy_state(state))
                    for f in funcs
                )
            )
            merged = self._merge_states(results)
            return merged, next_step_name

        parallel_step.__name__ = name
        self.steps[name] = parallel_step

    async def run(self, state: Optional[State] = None):
        if state is not None:
            self.state = state
        if self.state is None:
            raise ValueError("Initial state must be provided before running the flow.")

        current = self.start
        while current:
            step = self.steps.get(current)
            if not step:
                break
            self.state, current = await step(self.state)
            if self.enable_post and self.collector:
                self.collector((step.__name__, self.state, current, self.user_account, self.machine))


class ListCollector:
    def __init__(self):
        self.events: List[tuple] = []

    def __call__(self, event_tuple: Tuple):
        self.events.append(event_tuple)
