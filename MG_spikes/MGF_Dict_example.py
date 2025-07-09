import asyncio
import time  # noqa: F401
from copy import deepcopy  # noqa: F401
from typing import Any, Dict

from MiniGraphF import FlowFunc, FunctionalFlow, StepResult, ListCollector


# --- Step Definitions ---
async def setup(state: Dict[str, Any]) -> StepResult:
    state["number"] = -7
    return state, "parallel_start"


async def check_even(state: Dict[str, Any]) -> StepResult:
    await asyncio.sleep(0.5)
    is_odd = state["number"] % 2 != 0
    state["isOdd"] = str(is_odd)
    return state, state["isOdd"]


async def convert(state: Dict[str, Any]) -> StepResult:
    state["number"] *= -1
    return state, None


async def done(state: Dict[str, Any]) -> StepResult:
    return state, "print_state"


# --- Parallel Subtasks ---
async def call_barney(state: Dict[str, Any]) -> Dict[str, Any]:
    await asyncio.sleep(1.9)
    state["barney"] = "I'm Barney!"
    return state


async def call_other(state: Dict[str, Any]) -> Dict[str, Any]:
    await asyncio.sleep(1)
    state["other"] = "I'm someone else."
    return state


async def print_state(state: Dict[str, Any]) -> StepResult:
    print(state)
    return state, None


# --- NESTED FLOW EXAMPLE ---
async def begin(state: Dict[str, Any]) -> StepResult:
    print("inner_begin")
    return state, None


async def get_category(state: Dict[str, Any]) -> StepResult:
    state["inner_result"] = "Jim's Category Example!"
    return state, None


async def inner_flow(state: Dict[str, Any]) -> StepResult:
    inner_state = {
        "inner_name": "Inner",
        "inner_result": "NOPE!",
    }

    inner = FunctionalFlow()
    inner.initial_state(inner_state)
    inner.step(begin, next_step=get_category)
    inner.step(get_category)

    await inner.run()

    # Copy result to outer state
    state["category"] = inner.state["inner_result"]
    return state, None


# --- Run the Flow ---
if __name__ == "__main__":
    collector = ListCollector()

    flow = FunctionalFlow(default_timeout=3.0, collector=collector)

    # Dict-based state
    flow.initial_state({
        "number": 0,
        "barney": "",
        "other": "",
        "isOdd": "False",
        "category": ""
    })

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
        print(event)
