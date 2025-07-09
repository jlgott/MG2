import asyncio
import time  # noqa: F401
from copy import deepcopy
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union
from typing import Callable, Dict, Optional, Union, List
from MiniGraphF import FunctionalFlow, State, StepResult, FlowFunc
from pydantic import BaseModel


# --- Step Definitions ---
async def setup(state: State) -> StepResult:
    state.number = 8
    return state, "parallel_start"


async def check_even(state: State) -> StepResult:
    await asyncio.sleep(0.5)
    is_odd = state.number % 2 != 0
    state.isOdd = str(is_odd)
    return state, state.isOdd


async def convert(state: State) -> StepResult:
    state.number *= -1
    return state, None


async def done(state: State) -> StepResult:
    return state, "print_state"


# --- Parallel Subtasks ---
async def call_barney(state: State) -> State:
    await asyncio.sleep(1.9)
    state.barney = "I'm Barney!"
    return state


async def call_other(state: State) -> State:
    await asyncio.sleep(1)
    state.other = "I'm someone else."
    return state


async def print_state(state: State) -> StepResult:
    print(state)
    return state, None


#* NESTED EXAMPLE
# TODO Inner Flow TEST
async def begin(state):
    print("inner_begin")
    return state, None


async def get_category(state):
    state.inner_result = "Jim's Category Example!"
    return state, None


async def inner_flow(state: State) -> State:
    class InnerState(BaseModel):
        inner_name: str = "Inner"
        inner_result: str = "NOPE!"

    inner_state = InnerState()

    inner_flow = FunctionalFlow()
    inner_flow.initial_state(inner_state)
    inner_flow.step(begin, next_step=get_category)
    inner_flow.step(get_category)

    await inner_flow.run()

    # Copy result from inner flow to outer state
    state.category = inner_flow.state.inner_result

    return state, None


# --- Custom State Model ---
class MyState(BaseModel):
    number: int = 0
    barney: str = ""
    other: str = ""
    isOdd: str = "False"
    category: str = ""


# --- Simple Collector Example ---
class ListCollector:
    def __init__(self):
        self.events: List[Dict[str, Any]] = []

    def __call__(self, step_name: str, state: State, next_step: Optional[str]):
        # Defensive copy
        timestamp = time.time()
        snapshot = (
            state.model_copy(deep=True)
            if isinstance(state, BaseModel)
            else deepcopy(state)
        )
        self.events.append(
            {
                "timestamp": timestamp,
                "step_name": step_name,
                "state": snapshot,
                "next_step": next_step,
            }
        )


# --- Run the Flow ---
if __name__ == "__main__":
    collector = ListCollector()

    flow = FunctionalFlow(default_timeout=3.0, collector=collector)
    
    flow.initial_state(MyState())
    
    flow.step(setup, next_step="parallel_start")
    
    flow.parallel(
        "parallel_start",
        [call_barney, call_other],
        next_step=check_even,
        ignore_timeout=True,
    )
    
    flow.if_step(
        check_even, cases={"True": convert, "False": done}, ignore_timeout=True
    )
    
    flow.step(convert, next_step=done)
    
    flow.step(done, next_step=print_state)
    
    flow.step(print_state, next_step=inner_flow)
    
    flow.step(inner_flow)

    asyncio.run(flow.run())

    for event in collector.events:
        print(
            f"[TRACE @ {event['timestamp']:.3f}] from: {event['step_name']} → to: {event['next_step']}, state: {event['state']}"
        )
