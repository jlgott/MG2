from CFLOW import Flow, Node, ParallelNode
from pydantic import BaseModel, Field
import asyncio
import random
from typing import List, Dict, Optional, Union, Any
import time
from agents import Agent, Runner, RunConfig, function_tool, set_default_openai_api, RunContextWrapper
import functools
import numpy as np
from dotenv import load_dotenv

load_dotenv()
set_default_openai_api("chat_completions")


class LLMState(BaseModel):
    user_input: Union[str, List[str]] = None
    flow_path: str = "sequential"
    llm_responses: Union[str, List[str], BaseModel, Dict[str, Any]] = None
    agent_config: Dict[str, Any] = {}


def call_LLM(
    name: str = "DefaultAgent",
    model: str = "gpt-4.1-mini", 
    system_prompt: str = "",
    output_type: Union[BaseModel, Dict[str, Any]] = None,
    tools = [],
    timeout: int = 30,
    max_turns: int = 3
    ):

    def decorator(user_func):
        @functools.wraps(user_func)
        async def wrapper(*args, **kwargs):
            
            # Get user queries
            user_output = user_func(*args, **kwargs)
            
            # Build the flow graph
            async def start(state: LLMState) -> LLMState:
                state.user_input = user_output
                state.agent_config = {
                    "name": name,
                    "model": model,
                    "instructions": system_prompt,
                    "tools": tools or [],
                    "output_type": output_type or None
                }
                if isinstance(user_output, list):
                    state.flow_path = "parallel"
                return state
            
            async def sequential_llm(state: LLMState) -> LLMState:
                agent = Agent(**state.agent_config)
                result = await Runner.run(agent, state.user_input, max_turns=max_turns)
                state.llm_responses = result.final_output
                return state
            
            async def parallel_llm(state: LLMState) -> LLMState:
                agent = Agent(**state.agent_config)
                tasks = []
                for query in state.user_input:
                    tasks.append(Runner.run(agent, query, max_turns=max_turns))
                
                results = await asyncio.gather(*tasks)
                state.llm_responses = [r.final_output for r in results]
                return state

            async def finish(state: LLMState):
                print(f"State from Finish: {state}")
                return state


            # Create nodes
            start_node = Node(start)
            sequential_node = Node(sequential_llm)
            parallel_node = Node(parallel_llm)
            finish_node = Node(finish)


            # Build graph
            start_node - ("flow_path", "sequential") >> sequential_node
            start_node - ("flow_path", "parallel") >> parallel_node
            parallel_node >> finish_node
            sequential_node >> finish_node


            # Run flow
            flow = Flow()
            flow.set_start(start_node)
            
            initial_state = LLMState()
            result_state = await flow.run(initial_state)
            
            return result_state.llm_responses
        
        return wrapper
    return decorator

# Example usage
if __name__ == "__main__":

    print("\n")

    async def sequentials():
        ss = time.time()
        @call_LLM(system_prompt="Be helpful and brief")
        def single_query():
            return "What is the capital of France?"
        
        @call_LLM(system_prompt="Be helpful and brief")
        def single_query2():
            return "What is the capital of Spain?"
        
        result1 = await single_query()
        result2 = await single_query2()
        print(f"Sequential Time: {time.time() - ss:.3f}")
        print(f"Results: {result1}, {result2}")
        
    async def par():
        sp = time.time()
        @call_LLM(system_prompt="Be helpful and brief")
        def multiple_queries():
            return ["What is the capital of France?", "What is the capital of Spain?"]
        
        result = await multiple_queries()
        print(f"Parallel Time: {time.time() - sp:.3f}")
        print(f"Results: {result}")

    async def test():
        await sequentials()
        print("\n\n")
        await par()

    asyncio.run(test())