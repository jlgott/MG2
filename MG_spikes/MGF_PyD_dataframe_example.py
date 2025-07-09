import asyncio
import time  # noqa: F401
from copy import deepcopy  # noqa: F401
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

import pandas as pd
from pydantic import BaseModel, Field  # noqa: F401

from MiniGraphF import (  # noqa: F401
    FlowFunc,
    FunctionalFlow,
    ListCollector,
    State,
    StepResult,
)


# --- Step Definitions ---
async def load_df(state:State) -> State:
    df = pd.DataFrame([
        {"date": "2025-07-07", "amount": 23.99, "merchant": "Bookstore"},
        {"date": "2025-07-07", "amount": 60.00, "merchant": "Amazon"},
        {"date": "2025-07-07", "amount": 42.00, "merchant": "Walmart"},
        ])
    state.raw_data = df
    return state, None

async def transform_df(state:State) -> State:
    
    raw = state.raw_data
    df = (raw
            .assign(clowns=lambda df: 'clowns'+df["merchant"].astype(str))
    )
    state.transformed = df
    return state, None



# --- Custom State Model ---
class MyState(BaseModel):
    raw_data: pd.DataFrame = Field(default_factory=pd.DataFrame)
    transformed: Optional[Union[pd.DataFrame, Dict[str, str]]] = None

    class Config:
        arbitrary_types_allowed = True


# --- Run the Flow ---
if __name__ == "__main__":
    collector = ListCollector()

    flow = FunctionalFlow(default_timeout=3.0, collector=collector)
    
    flow.initial_state(MyState())

    flow.step(load_df, next_step=transform_df)
    flow.step(transform_df)
    
    asyncio.run(flow.run())

    for event in collector.events:
        print(event)
