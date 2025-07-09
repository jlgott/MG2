from typing import Optional, Type, Callable, List, Dict, Any
from pydantic import BaseModel
from agents import Agent, Runner, RunConfig
from MiniGraphF import FunctionalFlow
import functools
import asyncio


def llm_node(
    *,
    name: str = "DefaultAgentNode",
    prompt: str,
    model: str = "gpt-4.1-mini",
    output_model: Optional[Type[BaseModel]] = None,
    tools: Optional[List[Callable]] = None,
    retries: int = 3,
    timeout: float = 20,
    collector = None
    ):
    def decorator(user_func: Callable):
        agent = Agent(
            name=name,
            model=model,
            instructions=prompt,
            tools=tools or [],
            output_type=output_model
        )

        mini = FunctionalFlow(default_retries=retries, default_timeout=timeout, collector=collector)

        async def call_agent(state: Dict[str, Any]):
            input_dict = state.model_dump() if isinstance(state, BaseModel) else dict(state)

            # Use user-defined function to create prompt input
            prompt_input = user_func(input_dict)

            result = await Runner.run(
                agent,
                prompt_input,
                run_config=RunConfig(tracing_disabled=True),
            )
            # print(result, "\n")  #* DEBUG
            return result.final_output, None 

        call_agent.__name__ = user_func.__name__
        mini.step(call_agent)

        @functools.wraps(user_func)
        def wrapper(input_data):
            # Normalize input to always be a list of rows
            if isinstance(input_data, dict):
                rows = [input_data]
            elif isinstance(input_data, list):
                rows = input_data
            else:
                raise TypeError("Input must be a dict or list of dicts")

            results = []
            for row in rows:
                asyncio.run(mini.run(row))
                results.append(mini.state)

            return results if len(results) > 1 else results[0]

        return wrapper
    return decorator



#* EXAMPLE USAGE ----------------------------
class Transaction(BaseModel):
    date: str
    amount: float
    merchant: str

class TxSummary(BaseModel):
    summary: str
    category: str

@llm_node(
    prompt="Summarize this transaction in one sentence. Apply some type of categorization as well: online or brick_n_mortar",
    model="gpt-4.1-nano",
    output_model=TxSummary,
)
def summarize(tx: dict):
    return f"Transaction on {tx['date']} for ${tx['amount']} at {tx['merchant']}."

result = summarize(
    [{"date": "2025-07-07", "amount": 23.99, "merchant": "Bookstore"},
    {"date": "2025-07-07", "amount": 60.00, "merchant": "Amazon"},
    {"date": "2025-07-07", "amount": 42.00, "merchant": "Walmart"}]
)

print(result)

import pandas as pd  # noqa: E402

# Sample transaction data
df = pd.DataFrame([
    {"date": "2025-07-07", "amount": 23.99, "merchant": "Bookstore"},
    {"date": "2025-07-07", "amount": 60.00, "merchant": "Amazon"},
    {"date": "2025-07-07", "amount": 42.00, "merchant": "Walmart"},
])

# Option 1: Use apply row-wise (requires axis=1)
df["summary_obj"] = df.apply(lambda row: summarize(row.to_dict()), axis=1)
print(df)
