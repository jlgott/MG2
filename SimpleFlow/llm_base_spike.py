from simpleflow import SimpleFlow, State, StepFunc
import asyncio
from typing import Any, List, Union, TypedDict
from agents import Agent, function_tool, Runner, RunConfig
import asyncio
from agents import set_default_openai_api
from dotenv import load_dotenv
import numpy as np
from pydantic import BaseModel

load_dotenv()
set_default_openai_api("chat_completions")


# --- FlowState Definition ---
class FlowState(TypedDict, total=False):
    input: Union[List, Any]
    input_type: str
    semaphore_limit: int
    semaphore: asyncio.Semaphore

# # --- Mock LLM Function ---
# async def llm_fn(item):
#     await asyncio.sleep(0.5)
#     result = f"processed_{item}"
#     return result


async def llm_fn(query:Union[str,List]):
    agent = Agent(
        model="gpt-4.1-mini",
        name="LLNFN_AGENT_TEST",
        instructions="You are a helpful assisstant.",
        # tools=[person_detector],
        # # #* STRUCTURED OUTPUT TEST
        # output_type=StrucOutput,
    )

    result = await Runner.run(
        agent,
        query,
        run_config=RunConfig(tracing_disabled=False),
        max_turns=5
    )

    return result.final_output


# --- Flow Steps ---
async def check_input(state):
    if isinstance(state["input"], list):
        state["input_type"] = "list"
        state["semaphore"] = asyncio.Semaphore(state.get("semaphore_limit", 5))
    else:
        state["input_type"] = "single"
    return state

async def process_single(state):
    result = await llm_fn(state["input"])
    state["output_0"] = result
    return state

async def process_list(state):
    input_list = state["input"]
    
    # Execute parallel functions directly with semaphore control
    tasks = []
    for i, item in enumerate(input_list):
        async def process_item(item=item, index=i):
            async with state["semaphore"]:
                result = await llm_fn(item)
                state[f"output_{index}"] = result
        tasks.append(process_item())
    
    await asyncio.gather(*tasks)
    return state








async def end_graph(state):
    return state

# --- Main Flow ---
async def run_flow(input_data, semaphore_limit=5):
    state = {
        "input": input_data,
        "semaphore_limit": semaphore_limit
    }
    
    flow = SimpleFlow()
    flow.if_step(check_input, {"single": process_single, "list": process_list}, key="input_type")
    
    return await flow.run(state)

# --- Example ---
async def main():
    # Test list
    result1 = await run_flow(["Capital of France?", "Capital of Uganda?", "Capital of Ecuador?"], semaphore_limit=2)
    print("List result:", {k: v for k, v in result1.items() if k.startswith("output")})
    
    # Test single
    result2 = await run_flow("Capital of Texas?")
    print("Single result:", {k: v for k, v in result2.items() if k.startswith("output")})

if __name__ == "__main__":
    asyncio.run(main())