import functools
from typing import Callable, Dict, List, Optional, Union
import asyncio
from pydantic import BaseModel
from MiniGraphF import FlowFunc, FunctionalFlow, ListCollector, State

#* --- Decorators for MiniGraphF Steps ---
def flow_step(
    flow: FunctionalFlow,
    *,
    next_step: Union[str, Callable] = None,
    retries: Optional[int] = None,
    backoff: Optional[float] = None,
    timeout: Optional[float] = None,
    ignore_timeout: bool = False,
):
    """
    Decorator to register a function as a step in the flow.
    """
    def decorator(func: FlowFunc):
        @functools.wraps(func)
        async def wrapper(state: State):
            return await func(state)

        wrapper.raw_func = func  # Expose original for parallel use
        flow.step(
            wrapper,
            next_step=next_step,
            retries=retries,
            backoff=backoff,
            timeout=timeout,
            ignore_timeout=ignore_timeout,
        )
        return wrapper

    return decorator


def flow_if(
    flow: FunctionalFlow,
    *,
    cases: Dict[str, Union[str, Callable]],
    retries: Optional[int] = None,
    backoff: Optional[float] = None,
    timeout: Optional[float] = None,
    ignore_timeout: bool = False,
):
    """
    Decorator to register a branching step in the flow (if/case).
    """
    def decorator(func: FlowFunc):
        flow.if_step(
            func,
            cases=cases,
            retries=retries,
            backoff=backoff,
            timeout=timeout,
            ignore_timeout=ignore_timeout,
        )
        return func

    return decorator


def flow_parallel_step(
    flow: FunctionalFlow,
    *,
    name: str,
    funcs: List[Callable],
    merge: Optional[Callable] = None,
    next_step: Union[str, Callable] = None,
    timeout: Optional[float] = None,
    ignore_timeout: bool = False,
):
    """
    Function to register multiple parallel steps in the flow.
    This is not a decorator, but a named registration utility.

    This is not a decorator, but a named registration utility.
    """
    raw_funcs = [getattr(f, 'raw_func', f) for f in funcs]
    return flow.parallel(
        name=name,
        funcs=raw_funcs,
        merge=merge,
        next_step=next_step,
        timeout=timeout,
        ignore_timeout=ignore_timeout,
    )


# --- Example Usage ---

if __name__ == "__main__":

    class MyState(BaseModel):
        count: int = 0
        message: str = ""
        task1_result: Optional[str] = None
        task2_result: Optional[str] = None

    collector = ListCollector()
    flow = FunctionalFlow(collector=collector)

    @flow_step(flow, next_step="check")
    async def start(state: MyState):
        state.count += 1
        state.message = "Started"
        return state, None

    @flow_if(flow, cases={"even": "parallel_group", "odd": "parallel_group"})
    async def check(state: MyState):
        parity = "even" if state.count % 2 == 0 else "odd"
        state.message = f"Count is {parity}"
        return state, parity

    async def task1(state: MyState):
        state.task1_result = "task1 done"
        return state, None

    async def task2(state: MyState):
        state.task2_result = "task2 done"
        return state, None

    flow_parallel_step(
        flow,
        name="parallel_group",
        funcs=[task1, task2],
        next_step="done",
    )

    @flow_step(flow)
    async def done(state: MyState):
        state.message += " → Finished"
        return state, None

    initial = MyState(count=3)
    asyncio.run(flow.run(initial))
    print(flow.state)

    print("\n")

    for event in collector.events:
        print(event)
    print("\n")

    