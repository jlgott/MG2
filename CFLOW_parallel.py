from CFLOW import Flow, Node, ParallelNode
from pydantic import BaseModel, Field
import asyncio
import random
from typing import List, Dict, Optional
import time


# Example usage and testing
if __name__ == "__main__":

    def start(state):
        return state

    # Same processing functions for both demos
    async def process_a(state):
        print("Process A starting...")
        await asyncio.sleep(2)
        print("Process A completed!")
        state["result_a"] = "data_from_a"
        return state

    async def process_b(state):
        print("Process B starting...")
        await asyncio.sleep(1)
        print("Process B completed!")
        state["result_b"] = "data_from_b"
        return state

    async def process_c(state):
        print("Process C starting...")
        await asyncio.sleep(3)
        print("Process C completed!")
        state["result_c"] = "data_from_c"
        return state

    async def end(state):
        return state


    # DEFINE NODES
    
    start_node = Node(start)
    pstart_node = Node(start)
    node_a = Node(process_a)
    node_b = Node(process_b)
    node_c = Node(process_c)
    end_node = Node(end)

    parallel_node = ParallelNode([node_a, node_b, node_c])



    async def demo_sequential():
        print("=== Sequential Demo ===")
        start_time = time.time()
        
        sflow = Flow()
        sflow.set_start(start_node)

        start_node >> node_a >> node_b >> node_c >> end_node
        
        # Initial state with placeholder fields
        initial_state = {
            "input_data": "test_data",
            "result_a": None,
            "result_b": None, 
            "result_c": None
        }

        result = await sflow.run(initial_state)
        print(result)
        print(f"Sequential took: {time.time() - start_time:.2f}s")  # Should be ~6s
        return result

    async def demo_parallel():
        print("=== Parallel Demo ===")
        start_time = time.time()
        
        pflow = Flow()
        pflow.set_start(pstart_node)

        pstart_node >> parallel_node
        parallel_node >> end_node
        
        # Initial state with placeholder fields
        initial_state = {
            "input_data": "test_data",
            "result_a": None,
            "result_b": None, 
            "result_c": None
        }
        
        result = await pflow.run(initial_state)
        print(result)
        print(f"Parallel took: {time.time() - start_time:.2f}s")   # Should be ~3s
        return result
            

# Run both demos
async def main():
    await demo_sequential()
    await demo_parallel()
    
# Run example
asyncio.run(main())