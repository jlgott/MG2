import asyncio
import pytest
from typing import Dict, Any
from pydantic import BaseModel
from Simple_Flow_Engine import SimpleFlow

class MyModel(BaseModel):
    x: int = 0
    y: str = ""

@pytest.mark.asyncio
async def test_with_dict():
    async def step1(state: Dict[str, Any]) -> Dict[str, Any]:
        state["x"] = 1
        return state

    async def step2(state: Dict[str, Any]) -> Dict[str, Any]:
        state["y"] = "done"
        return state

    flow = SimpleFlow()
    flow.step(step1).step(step2)
    result = await flow.run({})

    assert result["x"] == 1
    assert result["y"] == "done"

@pytest.mark.asyncio
async def test_with_basemodel():
    async def step1(state: MyModel) -> MyModel:
        state.x = 42
        return state

    async def step2(state: MyModel) -> MyModel:
        state.y = "hello"
        return state

    flow = SimpleFlow()
    flow.step(step1).step(step2)
    result = await flow.run(MyModel())

    assert result.x == 42
    assert result.y == "hello"

@pytest.mark.asyncio
async def test_parallel_merge():
    async def fn1(state):
        if isinstance(state, BaseModel):
            state.x = 1
        else:
            state["x"] = 1
        return state

    async def fn2(state):
        if isinstance(state, BaseModel):
            state.y = "merged"
        else:
            state["y"] = "merged"
        return state

    flow = SimpleFlow()
    flow.parallel([fn1, fn2])
    result_dict = await flow.run({})
    assert result_dict["x"] == 1
    assert result_dict["y"] == "merged"

    result_model = await flow.run(MyModel())
    assert result_model.x == 1
    assert result_model.y == "merged"

@pytest.mark.asyncio
async def test_if_step_branching():
    async def branch_decider(state):
        if isinstance(state, BaseModel):
            state.branch = "case1"
        else:
            state["branch"] = "case1"
        return state

    async def case1(state):
        if isinstance(state, BaseModel):
            state.msg = "ran case1"
        else:
            state["msg"] = "ran case1"
        return state

    flow = SimpleFlow()
    flow.if_step(branch_decider, {"case1": case1}, key="branch")

    result_dict = await flow.run({})
    assert result_dict["msg"] == "ran case1"

    result_model = await flow.run(MyModel())
    assert result_model.msg == "ran case1"
