from typing import Optional, Type, Callable, List, Dict, Any, Union  # noqa: F401
from pydantic import BaseModel
from agents import Agent, Runner, RunConfig, function_tool
from simpleflow import State, StepFunc, SimpleFlow  # noqa: F401
import asyncio
import time
from functools import wraps
import functools

# --- Global Output Log ---
EVENT_LOG: List[str] = []

# def llm_node(
#     *,
#     name: str = "DefaultAgentNode",
#     prompt: str,
#     model: str = "gpt-4.1-mini",
#     output_model: Optional[Type[BaseModel]] = None,
#     tools: Optional[List[Callable]] = None,
#     retries: int = 3,
#     timeout: float = 20,
#     collector=None,
#     ):
#     def decorator(user_func: Callable):
#         agent = Agent(
#             name=name,
#             model=model,
#             instructions=prompt,
#             tools=tools or [],
#             output_type=output_model,
#         )

#         flow = SimpleFlow()

#         async def call_agent(state: State) -> State:
#             input_dict = (
#                 state.model_dump() if isinstance(state, BaseModel) else dict(state)
#             )
#             prompt_input = user_func(input_dict)

#             result = await Runner.run(
#                 agent,
#                 prompt_input,
#                 run_config=RunConfig(tracing_disabled=True),
#             )

#             if isinstance(state, BaseModel):
#                 return state.model_copy(update=result.final_output)
#             else:
#                 state.update(result.final_output)
#                 return state

#         flow.step(call_agent, retries=retries, timeout=timeout)

#         @functools.wraps(user_func)
#         def wrapper(input_data: dict) -> BaseModel:
#             if not isinstance(input_data, dict):
#                 raise TypeError("Input must be a dict")

#             result = asyncio.run(flow.run(input_data))
#             print(f"internal wrapper result type: {type(result)}:{result}")

#             if output_model:
#                 return output_model(**{
#                     k: v for k, v in result.items() if k in output_model.model_fields
#                 })
#             return result

#         return wrapper

#     return decorator


def llm_node(
    *,
    name: str = "DefaultAgent",
    prompt: str,
    model: str = "gpt-4.1",
    output_model: Optional[Type[BaseModel]] = None,
    tools: Optional[list[Callable]] = None,
    retries: int = 3,
    timeout: float = 20,
    state_model: Optional[Union[Type[BaseModel], Type[dict]]] = None,
    collector: Optional[Callable] = None
    ):
    # Determine if we should use a BaseModel
    use_model = isinstance(state_model, type) and issubclass(state_model, BaseModel)
    model_cls = state_model if use_model else None

    agent = Agent(
        name=name,
        model=model,
        instructions=prompt,
        output_type=output_model,
        tools=tools or [],
    )

    def decorator(user_func: Callable[[Dict[str, Any]], Union[str, list[str]]]):
        @functools.wraps(user_func)
        def wrapper(input_data: Union[Dict[str, Any], BaseModel]) -> Any:
            flow = SimpleFlow()

            async def start(state: State) -> State:
                if use_model and not isinstance(state, model_cls):
                    state = model_cls(**state)
                print("Start Node State:", state)
                return state

            async def call_llm(state: State) -> State:
                input_dict = state.model_dump() if isinstance(state, BaseModel) else dict(state)
                prompt_input = user_func(input_dict)
                print("Input Dict", prompt_input)

                result = await Runner.run(
                    agent,
                    prompt_input,
                    run_config=RunConfig(tracing_disabled=True),
                )

                if isinstance(result.final_output, dict) and use_model:
                    return state.model_copy(update=result.final_output)
                elif use_model:
                    raise TypeError(f"Expected {model_cls.__name__} from agent but got {type(result.final_output)}")
                else:
                    return result.final_output

            flow.step(start, timeout=timeout)
            flow.step(call_llm, retries=retries, timeout=timeout)

            final_state = asyncio.run(flow.run(input_data))
            return final_state

        return wrapper

    return decorator


# # * EXAMPLE USAGE ----------------------------
"""
If you pass a BaseModel as the output and you **SHOULD**, this is what the LLM will try and return as Dict[str, str].

"""

#* BE EXPLICIT!!! All Normal Prompt Engineering Rules Apply!!!
prompt = (
    "You are a transaction classifier. Your job is to:\n"
    "1. Classify the transaction as 'online' or 'brick_and_mortar'.\n"
    "2. Always send ONLY that classification (e.g., 'online') to the TOOL named 'check_online'.\n"
    "3. Do NOT send the full input or transaction description to the TOOL.\n"
    "4. Your final answer should be the TOOL's output only.\n"
    "\n"
    "Example:\n"
    "Input: Transaction at Amazon\n"
    "Thought: This is an online store.\n"
    "Action: check_online('online')\n"
    "Tool Result: ONLINE PURCHASE!\n"
    "Final Answer: ONLINE PURCHASE!"
)


class TxState(BaseModel):
    date: str
    amount: float
    merchant: str
    category: Optional[str] = None

@function_tool
def check_online(category: str):
    print(f"TOOL CALL: {category}")
    if "online" in category.lower():
        return "ONLINE PURCHASE!"
    return category

# @llm_node(
#     name="TxClassifier",
#     prompt="Categorize this transaction as online or brick_and_mortar. Then call the TOOL with the result and return that output.",
#     model="gpt-4.1-mini",
#     state_model=TxState,
#     tools=[check_online]
# )
# def summarize(row_dict: dict) -> TxState:
#     return TxState(**row_dict)

@llm_node(
    name="TxClassifier",
    prompt="Categorize this transaction as online or brick_and_mortar. Then call the TOOL with the result and return that output.",
    model="gpt-4.1-mini",
    output_model={},
    tools=[check_online]
)
def summarize(row_dict: dict) -> TxState:
    return TxState(**row_dict)


import pandas as pd  # noqa: E402
pd.set_option("display.max_colwidth", None)

# Sample transaction data
df = pd.DataFrame(
    [
        {"date": "2025-07-07", "amount": 23.99, "merchant": "Bookstore"},
        {"date": "2025-07-07", "amount": 60.00, "merchant": "Amazon"},
        {"date": "2025-07-07", "amount": 42.00, "merchant": "Walmart"},
    ]
)

# Option 1: Use apply row-wise (requires axis=1)
df["summary_obj"] = df.apply(lambda row: summarize(row.to_dict()), axis=1)
print(df)

[print(e) for e in EVENT_LOG]
