import asyncio
import os
import socket
import time  # noqa: F401
from copy import deepcopy  # noqa: F401
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

import pandas as pd
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
        if not isinstance(state, (dict, BaseModel)):
            raise TypeError(
                f"Initial state must be a dict or Pydantic BaseModel, not {type(state).__name__}"
            )
        self.state = state

    def _copy_state(self, state: State) -> State:
        if isinstance(state, BaseModel):
            return state.model_copy(deep=True)
        elif isinstance(state, dict):
            return deepcopy(state)
        raise TypeError(f"Unsupported state type for copying: {type(state)}")

    def _merge_states(
        self, states: List[Any], base_state: Optional[State] = None
    ) -> State:
        if isinstance(states[0], tuple):
            states = [s[0] for s in states]

        if self.custom_merge:
            return self.custom_merge(states)

        if isinstance(states[0], BaseModel):
            base = states[0].model_copy(deep=True)
            for s in states[1:]:
                base = base.model_copy(update=s.model_dump(exclude_unset=True))
            return base

        if isinstance(states[0], dict):
            base = base_state or {}
            merged = dict(base)

            for s in states:
                for k, v in s.items():
                    if k not in merged:
                        merged[k] = v
                    elif merged[k] != v:
                        original = base.get(k)
                        if merged[k] == original:
                            merged[k] = v  # accept first real change
                        elif v != original:
                            raise ValueError(
                                f"Conflict on key '{k}': '{merged[k]}' vs '{v}' (original: '{original}')"
                            )
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
                    local_state = self._copy_state(state)
                    if timeout:
                        new_state, _ = await asyncio.wait_for(
                            func(local_state), timeout=timeout
                        )
                    else:
                        new_state, _ = await func(local_state)
                    return new_state, next_step_name
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    if ignore_timeout:
                        return self._copy_state(state), next_step_name
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
                    local_state = self._copy_state(state)
                    if timeout:
                        new_state, branch = await asyncio.wait_for(
                            func(local_state), timeout=timeout
                        )
                    else:
                        new_state, branch = await func(local_state)
                    next_step = case_map.get(branch)
                    return new_state, next_step
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    if ignore_timeout:
                        return self._copy_state(state), None
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
                local_state = self._copy_state(input_state)
                if timeout:
                    result = await asyncio.wait_for(fn(local_state), timeout=timeout)
                else:
                    result = await fn(local_state)
                if self.collector:
                    self.collector(
                        (label, result, name, self.user_account, self.machine)
                    )
                return result
            except (asyncio.TimeoutError, asyncio.CancelledError):
                if ignore_timeout:
                    if self.collector:
                        self.collector(
                            (
                                label + ":timeout_skipped",
                                self._copy_state(input_state),
                                name,
                                self.user_account,
                                self.machine,
                            )
                        )
                    return self._copy_state(input_state)
                raise

        async def parallel_step(state: State) -> StepResult:
            base_state = self._copy_state(state)

            results = await asyncio.gather(
                *(tracked_func(f, self._get_func_name(f), state) for f in funcs)
            )
            merged = self._merge_states(results, base_state=base_state)
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
                self.collector(
                    (
                        step.__name__,
                        self.state,
                        current,
                        self.user_account,
                        self.machine,
                    )
                )


class ListCollector:  #* SIMPLE
    def __init__(self):
        self.events: List[tuple] = []

    def __call__(self, event_tuple: Tuple):
        self.events.append(event_tuple)


# class ListCollector:
#     def __init__(self):
#         self.events: List[tuple] = []

#     def summarize_value(self, val):
#         if isinstance(val, pd.DataFrame):
#             return {
#                 "_type": "DataFrame",
#                 "shape": val.shape,
#                 "columns": list(val.columns),
#                 "preview": val.head(2).to_dict(orient="records"),
#             }
#         return val

#     def summarize_state(self, state):
#         if isinstance(state, BaseModel):
#             state_dict = state.model_dump()
#         elif isinstance(state, dict):
#             state_dict = state
#         else:
#             return state  # leave unknown objects as-is

#         return {k: self.summarize_value(v) for k, v in state_dict.items()}

#     def __call__(self, event_tuple: Tuple):
#         label, state, step, user, machine = event_tuple
#         summarized_state = self.summarize_state(state)
#         self.events.append((label, summarized_state, step, user, machine))
