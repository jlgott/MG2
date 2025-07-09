import asyncio
import time  # noqa: F401
from copy import deepcopy
from typing import (  # noqa: F401, F811  # noqa: F401, F811
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

from pydantic import BaseModel  # noqa: F401

from MiniGraphF import FlowFunc, FunctionalFlow, State, StepResult, ListCollector  # noqa: F401


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
        # print(
        #     f"[TRACE @ {event['timestamp']:.3f}] from: {event['step_name']} -> to: {event['next_step']}, state: {event['state']}"
        # )
        print(event)
