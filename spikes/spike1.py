import asyncio
import copy
from typing import Any, Dict, List, Optional, Union


class Node:
    def __init__(self, name: str = None):
        self.name = name or f"node_{id(self)}"
        self.successors: List['Node'] = []
        self.params: Dict[str, Any] = {}
    
    def set_params(self, params: Dict[str, Any]):
        self.params = params
        return self
    
    async def prep(self, shared: Dict[str, Any]) -> Any:
        """Prepare/initialize this node - override this for setup logic"""
        return None
    
    async def execute(self, prep_result: Any, shared: Dict[str, Any]) -> Any:
        """Main execution logic - override this"""
        return prep_result
    
    async def post(self, exec_result: Any, shared: Dict[str, Any]) -> Any:
        """Post-process results"""
        return exec_result
    
    async def run(self, shared: Dict[str, Any]) -> Any:
        """Execute this node"""
        prep_result = await self.prep(shared)
        exec_result = await self.execute(prep_result, shared)
        return await self.post(exec_result, shared)
    
    def __rshift__(self, other: 'Node') -> 'Node':
        """node1 >> node2 syntax"""
        self.successors.append(other)
        return other


class Runner:
    @staticmethod
    async def async_run(start_node: Node, shared: Dict[str, Any] = None) -> Any:
        """Run graph asynchronously with automatic parallelization"""
        if shared is None:
            shared = {}
        
        return await Runner._execute_graph(start_node, shared)
    
    @staticmethod
    def sync(start_node: Node, shared: Dict[str, Any] = None) -> Any:
        """Synchronous wrapper around async execution"""
        return asyncio.run(Runner.async_run(start_node, shared))
    
    @staticmethod
    async def _execute_graph(node: Node, shared: Dict[str, Any], visited: set = None) -> Any:
        """Execute node and its successors with automatic parallelization"""
        if visited is None:
            visited = set()
        
        # Avoid cycles
        if id(node) in visited:
            return None
        visited.add(id(node))
        
        # Execute current node
        result = await node.run(shared)
        
        # If no successors, we're done
        if not node.successors:
            return result
        
        # If multiple successors, run them in parallel
        if len(node.successors) > 1:
            tasks = [
                Runner._execute_graph(copy.deepcopy(successor), shared, visited.copy())
                for successor in node.successors
            ]
            await asyncio.gather(*tasks)
        else:
            # Single successor, run sequentially
            await Runner._execute_graph(node.successors[0], shared, visited)
        
        return result

