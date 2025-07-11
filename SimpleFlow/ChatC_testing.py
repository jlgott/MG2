import functools

def my_decorator(verbose:bool=True):
    def actual_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            

            return func(*args, **kwargs)
        return wrapper
    return actual_decorator



# def retry(times=3, delay=1):
#     def decorator(func):
#         @functools.wraps(func)
#         def wrapper(*args, **kwargs):
#             for attempt in range(times):
#                 try:
#                     return func(*args, **kwargs)
#                 except Exception as e:
#                     print(f"Attempt {attempt+1} failed: {e}")
#                     time.sleep(delay)
#             raise Exception(f"Failed after {times} retries")
#         return wrapper
#     return decorator



from agents import Agent, function_tool, Runner, RunConfig
import asyncio
from agents import set_default_openai_api
from dotenv import load_dotenv
import numpy as np
from pydantic import BaseModel

load_dotenv()

set_default_openai_api("chat_completions")

# bootleg global for testing
user_list = ["Bob", "Alice", "Martha", "Frank"]
ALL_USER_LIST = []    
for user in user_list:
    ALL_USER_LIST.append(user+"_A")
    ALL_USER_LIST.append(user+"_B")
    ALL_USER_LIST.append(user+"_C")

print(ALL_USER_LIST)


TOOL_NUM = 0

@function_tool
def person_detector(text: str) -> str:
    """Takes text and guesses the user thru an ML model.  Also returns confidence level.
    
    Args: text = the text to be examined

    Returns:
        String:  with the guessed User and the confidnce level.
    """
    user = np.random.choice(ALL_USER_LIST)
    confidence = np.random.uniform(0,1) 
    tool_return = f"Suspected User: {user}, Confidence: {confidence:.3f}"

    if confidence < .50:
        global TOOL_NUM
        TOOL_NUM += 1
        print(f"\n***LOW CONFIDENCE TOOL CALL\n{tool_return} ----   {TOOL_NUM} \n")

    return tool_return


class StrucOutput(BaseModel):
    user:str
    confidence:float
    comments: str


# ChatAgent uses chat completions internally
agent = Agent(
    model="gpt-4.1",
    name="UserDetectorAgent_TEST",
    instructions="""Always call the person_detector tool and return its result.
        If the confidence is low, below 50%, try the tool again.
        Keep trying till you get a high confidence Score!!!
        Give the confidence score in your output always.
        """,
    tools=[person_detector],
    # #* STRUCTURED OUTPUT TEST
    output_type=StrucOutput,

)

async def main():
    # This is a lightweight chat-completion-backed run
    result = await Runner.run(
        agent,
        "Can you guess who I am from how I am typing?",
        run_config=RunConfig(tracing_disabled=False),
        max_turns=15
    )
    print("🤖 Agent Output:")
    print(f"RESULT:\n{result}")
    print(f"Output Type: {type(result.final_output)}")
    print("\n")
    print(result.raw_responses)
    print("\n")
    print("\n")

asyncio.run(main())
