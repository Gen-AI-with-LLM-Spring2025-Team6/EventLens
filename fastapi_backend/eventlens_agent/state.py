from typing import Annotated, Dict, List, Optional, Set, TypedDict, Any
from langgraph.graph.message import add_messages

class State(TypedDict):
    """State for the event recommendation chatbot agent."""
    # Conversation history
    messages: Annotated[List, add_messages]
    # Tools that have been invoked
    tool_calls: Optional[List[Dict]]
    # Results from all tools
    tool_results: Optional[Dict]
    # Current active tool being processed
    current_tool: Optional[str]
    # Tools that need to be called
    tools_to_call: Optional[List[str]]
    # Tools that have already been called
    tools_called: Optional[List[str]]
    # Final answer being constructed
    final_answer: Optional[str]
    # Boolean to track if we need to generate final answer
    need_final_answer: Optional[bool]