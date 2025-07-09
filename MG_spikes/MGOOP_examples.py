import asyncio
import warnings
from pydantic import BaseModel, Field
from agents import Agent, Runner, RunConfig
from dotenv import load_dotenv
from typing import Any, List, Dict, Tuple, Optional
import os
import json

# These disable default OpenAI logging
os.environ["OPENAI_AGENTS_DISABLE_TRACING"] = "1"
os.environ["LOGFIRE_SEND_TO_LOGFIRE"] = "false"

load_dotenv()


class AsyncNode:
    def __init__(self, max_retries=1, wait=0):
        self.params = {}
        self.successors = {}
        self.max_retries = max_retries
        self.wait = wait

    def set_params(self, params):
        self.params = params or {}

    def next(self, node, action="default"):
        self.successors.setdefault(action, [])
        self.successors[action].append(node)
        return node

    async def prep(self, shared):
        return shared

    async def execute(self, prep_res):
        return None

    async def post(self, shared, *args):
        return "default"

    async def _run_async(self, shared):
        prep_res = await self.prep(shared)
        for i in range(self.max_retries):
            try:
                exec_res = await self.execute(prep_res)
                break
            except Exception as e:
                if i == self.max_retries - 1:
                    raise e
                if self.wait > 0:
                    await asyncio.sleep(self.wait)

        try:
            return await self.post(shared)
        except TypeError:
            return await self.post(shared, prep_res, exec_res)

    async def run_async(self, shared):
        return await self._run_async(shared)

    def __rshift__(self, other):
        return self.next(other)

    def __sub__(self, action):
        return _ConditionalTransition(self, action)


class _ConditionalTransition:
    def __init__(self, src, action):
        self.src = src
        self.action = action

    def __rshift__(self, tgt):
        return self.src.next(tgt, self.action)


class Flow:
    def __init__(self, start_node):
        self.start_node = start_node

    async def run_async(self, shared=None):
        shared = shared or {}
        await self._run_from_node(self.start_node, shared)

    async def _run_from_node(self, node, shared):
        current = node
        while current:
            current.set_params({})
            action = await current._run_async(shared)
            next_nodes = current.successors.get(action)

            if not next_nodes:
                break
            elif len(next_nodes) == 1:
                current = next_nodes[0]
            else:
                await asyncio.gather(*(self._run_from_node(n, shared) for n in next_nodes))
                return

    def run_sync(self, shared=None):
        return asyncio.run(self.run_async(shared))


#* EXAMPLES

agent = Agent(
    name = "BarneyAgent",
    instructions = "You are a helpful assitant named Barney!",
    model = "gpt-4.1"
)

async def call_llm(agent: Agent, query:Any = None, tools: List[Any] = []) -> Tuple[Any, Any]:
    response = await Runner.run(
        agent,
        query,
        run_config=RunConfig(tracing_disabled=True),
    )

    return response, response.final_output




class Setup(AsyncNode):
    async def prep(self, shared):
        shared.number = 7
        return shared

    async def execute(self, shared):
        return shared


class CheckEven(AsyncNode):
    async def execute(self, shared):
        await asyncio.sleep(0.5)
        shared.isOdd = str(shared.number % 2 != 0)
        return shared

    async def post(self, shared):  # ✅ simplified signature
        return shared.isOdd    # → "True" or "False"


class ConvertOdd(AsyncNode):
    async def execute(self, shared):
        shared.number *= -1
        print("Converted to even-like:", shared.number)
        return shared


class Done(AsyncNode):
    async def execute(self, shared):
        print("✅ Done node executed.")
        print("🔚 Final shared state:", shared)
        print("Final state:", shared.model_dump_json())
        return shared


class LLMNode(AsyncNode):
    async def execute(self, shared):
        print("Calling Barney")
        try:
            _, response = await call_llm(
                agent=agent,
                query="Tell me who you are."
            )
            print(response)
            shared.memory.append(response)
        except Exception as e:
            print(f"❌ LLM call failed: {e}")
            shared.memory.append(f"[ERROR] {str(e)}")
        return shared


#* State Class
class SharedState(BaseModel):
    number : int = 0
    isOdd : str = "False"
    memory : Optional[List[str]] = Field(default_factory=list)


async def main():
    print("🚀 Flow is starting")
 
    shared = SharedState()

    start = Setup()
    check = CheckEven()
    fix_odd = ConvertOdd()
    ask_barney = LLMNode()
    end = Done()

    start >> check
    start >> ask_barney
    check - "True" >> fix_odd
    # check >> end
    # fix_odd >> end
    ask_barney >> end

    flow = Flow(start)

    # flow.run_sync(shared)
    await flow.run_async(shared)

if __name__ == "__main__":
    asyncio.run(main())
    # main()