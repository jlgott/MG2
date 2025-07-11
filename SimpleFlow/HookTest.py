from agents import Agent, function_tool, Runner, RunConfig
from agents.lifecycle import RunHooks
import json
import asyncio
from dotenv import load_dotenv

load_dotenv()


# Sample tool
@function_tool
def echo_tool(text: str) -> str:
    """Echoes back the text"""
    return f"ECHOOOOO!!!\n---::i: {text}"

# from agents.lifecycle import RunHooks
# import json

class MyHooks(RunHooks):
    async def on_tool_start(self, context, agent, tool):
        print("🔧 TOOL START")
        print(f"Tool: {tool.name}")
        print(context.__dict__)  # ✅ See what's actually inside

        try:
            tool_input = context.step.tool_call.input
            print("Tool Input:")
            print(tool_input.model_dump() if hasattr(tool_input, "model_dump") else tool_input)
        except Exception as e:
            print(f"⚠️ Failed to access tool input: {e}")

    async def on_tool_end(self, context, agent, tool, result):
        print("✅ TOOL END")
        print(f"Tool: {tool.name}")
        print("Output:", result)


# Agent definition
agent = Agent(
    name="HookTestAgent",
    model="gpt-4o",
    tools=[echo_tool],
)

# Main runner
async def main():
    # Build the Run object manually with hooks
    result = await Runner.run(
        starting_agent = agent,
        input="Say: 'This is a hook test'",
        hooks=MyHooks(),  # ✅ This is where hooks go now
        run_config=RunConfig(tracing_disabled=True),
    )


    print("\n🧠 FINAL RESPONSE:")
    print(result)

asyncio.run(main())
