import asyncio
import warnings


class AsyncNode:
    def __init__(self, max_retries=1, wait=0):
        self.params = {}
        self.successors = {}
        self.max_retries = max_retries
        self.wait = wait

    def set_params(self, params):
        self.params = params or {}

    def next(self, node, action="default"):
        if action in self.successors:
            warnings.warn(f"Overwriting successor for action '{action}'")
        self.successors[action] = self.successors.get(action, []) + [node]
        return node

    async def prep(self, shared):
        return shared

    async def execute(self, prep_res):
        return None

    async def post(self, shared, prep_res, exec_res):
        return "default"  # signals which branch to take next

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

        return await self.post(shared, prep_res, exec_res)

    async def run_async(self, shared):
        if self.successors:
            warnings.warn("Successors won't be run. Use Flow.")
        return await self._run_async(shared)

    def __rshift__(self, other):
        return self.next(other)

    def __sub__(self, action):
        if isinstance(action, str):
            return _ConditionalTransition(self, action)
        raise TypeError("Action must be a string")


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
        current = self.start_node
        action = "default"

        while current:
            current.set_params({})
            action = await current._run_async(shared)
            next_nodes = current.successors.get(action)

            if not next_nodes:
                break
            elif len(next_nodes) == 1:
                current = next_nodes[0]
            else:
                await asyncio.gather(*(n._run_async(shared) for n in next_nodes))
                break  # Parallel branches are terminal unless managed manually


    def run_sync(self, shared=None):
        return asyncio.run(self.run_async(shared))
