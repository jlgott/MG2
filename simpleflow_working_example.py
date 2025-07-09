# simpleflow_working_example
from simpleflow import State, StepFunc, SimpleFlow  # noqa: F401
from pydantic import BaseModel, Field  # noqa: F401
import asyncio
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union  # noqa: F401
import time

# --- Global Output Log ---
EVENT_LOG: List[str] = []


# --- Sample Usage ---
class MyModel(BaseModel):
    name: str = ""
    age: int = 0
    path: str = "BaseModel"
    a: int = 0
    b: int = 0
    choice: str = ""

    # class Config:
    #     extra = "allow"  #! NOT BEST PRACTICE ALLOWS MUTABLE STATE


async def start_step(state):
    timestamp = time.time()
    EVENT_LOG.append(f"[{timestamp:.2f}] start_step: {state}")
    return state


async def task_a(state):
    timestamp = time.time()
    EVENT_LOG.append(f"[{timestamp:.2f}] task_a_start: {state}")
    await asyncio.sleep(1)
    if isinstance(state, dict):
        state["a"] = 1
    else:
        state.a = 1
    timestamp = time.time()
    EVENT_LOG.append(f"[{timestamp:.2f}] task_a_end: {state}")
    return state


async def task_b(state):
    timestamp = time.time()
    EVENT_LOG.append(f"[{timestamp:.2f}] task_b_start: {state}")
    if isinstance(state, dict):
        state["b"] = 2
    else:
        state.b = 2
    timestamp = time.time()
    EVENT_LOG.append(f"[{timestamp:.2f}] task_b_end: {state}")
    return state


async def check_type(state):
    if isinstance(state, dict):
        state["choice"] = "A"
    else:
        state.choice = "B"
    timestamp = time.time()
    EVENT_LOG.append(f"[{timestamp:.2f}] check_type: {state}")
    return state


async def handle_type_a(state):
    timestamp = time.time()
    EVENT_LOG.append(f"[{timestamp:.2f}] handle_type_a: {state}")
    return state


async def handle_type_b(state):
    timestamp = time.time()
    EVENT_LOG.append(f"[{timestamp:.2f}] handle_type_b: {state}")
    return state


async def run_examples():
    dict_state = {"name": "Alice", "age": 30, "path": "DictModel"}
    flow1 = SimpleFlow()
    flow1.step(start_step)
    flow1.parallel([task_a, task_b])
    flow1.if_step(check_type, {"A": handle_type_a, "B": handle_type_b}, key="choice")
    await flow1.run(dict_state)

    model_state = MyModel(name="Bob", age=40)
    flow2 = SimpleFlow()
    flow2.step(start_step)
    flow2.parallel([task_a, task_b])
    flow2.if_step(check_type, {"A": handle_type_a, "B": handle_type_b}, key="choice")
    await flow2.run(model_state)

    print("\n--- EVENT LOG ---")
    for event in EVENT_LOG:
        print(event)


if __name__ == "__main__":
    asyncio.run(run_examples())
