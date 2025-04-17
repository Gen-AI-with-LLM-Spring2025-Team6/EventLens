import json
from typing import Dict, List, Optional, Any
import sys
import os
from datetime import datetime, timedelta
from langgraph.graph import StateGraph, START, END
from langgraph.prebuilt import ToolExecutor
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage
from langchain_core.prompts import ChatPromptTemplate
from fastapi_backend.eventlens_agent.state import State
from fastapi_backend.eventlens_agent.tools.rag_data_retreival_api import retrieve_events
from fastapi_backend.eventlens_agent.tools.weather_api import check_weather_for_address
from fastapi_backend.eventlens_agent.tools.maps_api import get_directions
from fastapi_backend.eventlens_agent.tools.sentiment_serpapi import format_reviews_for_llm

class EventRecommendationGraph:
    """
    A complete event recommendation system using LangGraph.
    """
    
    def __init__(self, llm_model: str = "gpt-4"):
        """
        Initialize the event recommendation system.
        
        Args:
            llm_model: The model to use for the LLM.
        """
        # Set up the LLM
        self.llm = ChatOpenAI(model=llm_model)
        
        # Set up tools
        self.tools = {
            "retrieve_events": {
                "func": retrieve_events,
                "description": "Finds relevant events based on user interests and preferences"
            },
            "check_weather": {
                "func": check_weather_for_address,
                "description": "Checks weather forecast for a specific location and date"
            },
            "get_directions": {
                "func": get_directions,
                "description": "Gets directions and travel information between two locations"
            },
            "get_event_reviews": {
                "func": format_reviews_for_llm,
                "description": "Gets reviews and sentiment about an event"
            }
        }
        
        # Wrap tools for execution - commenting out as your tools might have different signatures
        # self.tool_executor = ToolExecutor({
        #    name: tool for name, tool in self.tools.items()
        # })
        
        # Create the graph
        self.graph = self._build_graph()
    
    def _build_graph(self) -> StateGraph:
        """
        Build the LangGraph for the event recommendation system.
        
        Returns:
            StateGraph: The compiled graph.
        """
        # Create the graph builder
        graph_builder = StateGraph(State)
        
        # Add nodes to the graph
        graph_builder.add_node("controller", self._controller_node)
        graph_builder.add_node("rag_tool", self._rag_tool_node)
        graph_builder.add_node("weather_tool", self._weather_tool_node)
        graph_builder.add_node("maps_tool", self._maps_tool_node)
        graph_builder.add_node("reviews_tool", self._reviews_tool_node)
        graph_builder.add_node("final_answer", self._final_answer_node)
        
        # Define the edges of the graph
        # Start -> Controller
        graph_builder.add_edge(START, "controller")
        
        # Controller routes to appropriate tool or final answer
        graph_builder.add_conditional_edges(
            "controller",
            self._route_to_next_tool,
            {
                "rag_tool": "rag_tool",
                "weather_tool": "weather_tool",
                "maps_tool": "maps_tool",
                "reviews_tool": "reviews_tool",
                "final_answer": "final_answer",
                None: END  # If no more tools needed and no final answer needed
            }
        )
        
        # Each tool routes back to controller to decide next step
        graph_builder.add_edge("rag_tool", "controller")
        graph_builder.add_edge("weather_tool", "controller")
        graph_builder.add_edge("maps_tool", "controller")
        graph_builder.add_edge("reviews_tool", "controller")
        
        # Final answer -> END
        graph_builder.add_edge("final_answer", END)
        
        # Compile the graph
        return graph_builder.compile()
    
    def _controller_node(self, state: State) -> Dict:
        """
        Controller node that decides which tools to use based on the user's query.
        """
        # Initialize tools_to_call and tools_called if not present
        if state.get("tools_to_call") is None:
            state["tools_to_call"] = []
        
        if state.get("tools_called") is None:
            state["tools_called"] = []
        
        # If this is the first time, determine which tools to call
        if not state.get("tools_called"):
            return self._determine_tools_to_call(state)
        
        # If we've already called some tools, check if we need more
        return self._check_if_more_tools_needed(state)
    
    def _determine_tools_to_call(self, state: State) -> Dict:
        """
        Analyze the user query and determine which tools to call.
        """
        messages = state["messages"]
        
        # Extract the last user message
        last_user_message = None
        for msg in reversed(messages):
            if isinstance(msg, HumanMessage):
                last_user_message = msg.content
                break
        
        if not last_user_message:
            return {"need_final_answer": True}
        
        # Prepare messages for the LLM
        prompt = ChatPromptTemplate.from_messages([
            ("system", """You are an event recommendation assistant. 
            Your job is to determine which tools to use based on the user's query.
            You have the following tools available:
            
            - retrieve_events: Finds relevant events based on user interests and preferences from the database
            - check_weather: Checks weather forecast for a specific location and date
            - get_directions: Gets directions and travel information between two locations
            - get_event_reviews: Gets reviews and sentiment about an event
            
            Analyze the user's query and determine which tools are needed in what order.
            Return a JSON list of tool names in the order they should be used.
            Only include tools that are necessary for the user's query.
            """),
            ("user", last_user_message),
        ])
        
        response = self.llm.invoke(prompt)
        
        try:
            # Parse the tools to be used from the LLM response
            tools_to_call = json.loads(response.content)
            
            if isinstance(tools_to_call, dict) and "tools" in tools_to_call:
                tools_to_call = tools_to_call["tools"]
            
            if not isinstance(tools_to_call, list):
                tools_to_call = [tools_to_call]
            
            # Filter out any invalid tool names
            valid_tools = list(self.tools.keys())
            tools_to_call = [tool for tool in tools_to_call if tool in valid_tools]
            
            # If no valid tools were identified, go straight to final answer
            if not tools_to_call:
                return {"need_final_answer": True}
            
            return {
                "tools_to_call": tools_to_call,
                "tools_called": [],
                "tool_results": {},
                "current_tool": tools_to_call[0]  # Set the first tool to call
            }
        except:
            # If parsing fails, move to final answer
            return {"need_final_answer": True}
    
    def _check_if_more_tools_needed(self, state: State) -> Dict:
        """
        After a tool has been called, check if we need to call more tools.
        """
        tools_to_call = state.get("tools_to_call", [])
        tools_called = state.get("tools_called", [])
        
        # Get the next tool to call
        next_tools = [t for t in tools_to_call if t not in tools_called]
        
        if not next_tools:
            # No more tools to call, generate final answer
            return {"need_final_answer": True}
        
        return {"current_tool": next_tools[0]}
    
    def _route_to_next_tool(self, state: State) -> str:
        """
        Determine the next node to route to based on the current state.
        """
        # If we need a final answer, route to final answer node
        if state.get("need_final_answer"):
            return "final_answer"
        
        # Otherwise, route to the current tool
        current_tool = state.get("current_tool")
        
        if current_tool == "retrieve_events":
            return "rag_tool"
        elif current_tool == "check_weather":
            return "weather_tool"
        elif current_tool == "get_directions":
            return "maps_tool"
        elif current_tool == "get_event_reviews":
            return "reviews_tool"
        else:
            # Default to final answer if no tool is specified
            return "final_answer"
    
    def _extract_tool_params(self, state: State, tool_name: str) -> Dict:
        """
        Extract parameters for a specific tool from the user's messages.
        """
        messages = state["messages"]
        
        # Extract the last user message
        last_user_message = None
        for msg in reversed(messages):
            if isinstance(msg, HumanMessage):
                last_user_message = msg.content
                break
        
        if not last_user_message:
            return {}
        
        # Create a prompt to extract parameters for the specific tool
        tool_description = self.tools[tool_name]["description"]
        
        prompt = ChatPromptTemplate.from_messages([
            ("system", f"""Extract parameters for the {tool_name} tool from the user's message.
            This tool {tool_description}.
            
            Return a JSON object with parameters appropriate for this tool.
            
            For retrieve_events: {{
                "query": "user's request/interests"
            }}
            
            For check_weather: {{
                "address": "location address",
                "date": "date in YYYY-MM-DD format"
            }}
            
            For get_directions: {{
                "origin": "starting location",
                "destination": "ending location",
                "mode": "driving/walking/transit"
            }}
            
            For get_event_reviews: {{
                "event_name": "name of the event",
                "location": "location of the event"
            }}
            
            Only include parameters that can be directly inferred from the user's message.
            """),
            ("user", last_user_message),
        ])
        
        response = self.llm.invoke(prompt)
        
        try:
            # Parse the parameters from the LLM response
            parameters = json.loads(response.content)
            return parameters
        except:
            # If parsing fails, return empty parameters
            return {}
    
    def _rag_tool_node(self, state: State) -> Dict:
        """
        Execute the RAG tool to retrieve relevant events.
        """
        current_tool = state.get("current_tool")
        if current_tool != "retrieve_events":
            return {}
        
        # Extract parameters for the RAG tool
        params = self._extract_tool_params(state, "retrieve_events")
        
        # Execute the RAG tool
        query = params.get("query", "events")
        result = retrieve_events(query)
        
        # Update the state
        tools_called = state.get("tools_called", [])
        tools_called.append("retrieve_events")
        
        tool_results = state.get("tool_results", {})
        tool_results["retrieve_events"] = result
        
        return {
            "tools_called": tools_called,
            "tool_results": tool_results
        }
    
    def _weather_tool_node(self, state: State) -> Dict:
        """
        Execute the weather tool to get weather forecasts.
        """
        current_tool = state.get("current_tool")
        if current_tool != "check_weather":
            return {}
        
        # Extract parameters for the weather tool
        params = self._extract_tool_params(state, "check_weather")
        
        # Execute the weather tool
        address = params.get("address", "")
        date = params.get("date", "")
        
        if not address:
            # If no address is provided, use any location from RAG results
            events_result = state.get("tool_results", {}).get("retrieve_events", "")
            if "Boston" in events_result:
                address = "Boston, MA"
            elif "New York" in events_result:
                address = "New York, NY"
            elif "San Francisco" in events_result:
                address = "San Francisco, CA"
        
        if not date:
            # If no date is provided, use tomorrow's date
            tomorrow = datetime.now() + timedelta(days=1)
            date = tomorrow.strftime("%Y-%m-%d")
        
        result = check_weather_for_address(address, date)
        
        # Update the state
        tools_called = state.get("tools_called", [])
        tools_called.append("check_weather")
        
        tool_results = state.get("tool_results", {})
        tool_results["check_weather"] = result
        
        return {
            "tools_called": tools_called,
            "tool_results": tool_results
        }
    
    def _maps_tool_node(self, state: State) -> Dict:
        """
        Execute the maps tool to get directions.
        """
        current_tool = state.get("current_tool")
        if current_tool != "get_directions":
            return {}
        
        # Extract parameters for the maps tool
        params = self._extract_tool_params(state, "get_directions")
        
        # Execute the maps tool
        origin = params.get("origin", "")
        destination = params.get("destination", "")
        mode = params.get("mode", "driving")
        
        if not destination:
            # If no destination is provided, use any location from RAG results
            events_result = state.get("tool_results", {}).get("retrieve_events", "")
            if "Boston" in events_result:
                destination = "Boston, MA"
            elif "New York" in events_result:
                destination = "New York, NY"
            elif "San Francisco" in events_result:
                destination = "San Francisco, CA"
        
        result = get_directions(origin, destination, mode)
        
        # Update the state
        tools_called = state.get("tools_called", [])
        tools_called.append("get_directions")
        
        tool_results = state.get("tool_results", {})
        tool_results["get_directions"] = result
        
        return {
            "tools_called": tools_called,
            "tool_results": tool_results
        }
    
    def _reviews_tool_node(self, state: State) -> Dict:
        """
        Execute the reviews tool to get event reviews.
        """
        current_tool = state.get("current_tool")
        if current_tool != "get_event_reviews":
            return {}
        
        # Extract parameters for the reviews tool
        params = self._extract_tool_params(state, "get_event_reviews")
        
        # Execute the reviews tool
        event_name = params.get("event_name", "")
        location = params.get("location", "")
        
        if not event_name:
            # If no event name is provided, use any event from RAG results
            events_result = state.get("tool_results", {}).get("retrieve_events", "")
            if "Tech Conference" in events_result:
                event_name = "Tech Conference"
            elif "Music Festival" in events_result:
                event_name = "Music Festival"
            elif "Food Fair" in events_result:
                event_name = "Food Fair"
        
        result = format_reviews_for_llm(event_name, location)
        
        # Update the state
        tools_called = state.get("tools_called", [])
        tools_called.append("get_event_reviews")
        
        tool_results = state.get("tool_results", {})
        tool_results["get_event_reviews"] = result
        
        return {
            "tools_called": tools_called,
            "tool_results": tool_results
        }
    
    def _final_answer_node(self, state: State) -> Dict:
        """
        Generate the final answer based on the results from all tools.
        """
        messages = state["messages"]
        tool_results = state.get("tool_results", {})
        
        # Create a prompt for the final answer
        prompt = ChatPromptTemplate.from_messages([
            ("system", """You are an event recommendation assistant.
            Synthesize the results from the tools into a coherent, helpful response for the user.
            Use all the available information to provide a complete response.
            Be conversational and friendly.
            
            Include specific details from the tools' results when available:
            - Event details from the retrieve_events tool
            - Weather information from the check_weather tool
            - Directions and travel info from the get_directions tool
            - Reviews and sentiment from the get_event_reviews tool
            
            Only mention tools that were actually used.
            """),
            *messages,
            ("system", f"Tool results: {json.dumps(tool_results)}"),
        ])
        
        response = self.llm.invoke(prompt)
        
        # Return the final answer
        return {"messages": [AIMessage(content=response.content)]}
    
    def invoke(self, message: str) -> str:
        """
        Invoke the event recommendation system with a user message.
        
        Args:
            message: The user's message.
            
        Returns:
            str: The assistant's response.
        """
        # Create the initial state with the user's message
        state = {
            "messages": [HumanMessage(content=message)]
        }
        
        # Run the graph
        result = self.graph.invoke(state)
        
        # Return the last message from the assistant
        messages = result.get("messages", [])
        if messages:
            return messages[-1].content
        
        return "I'm sorry, I wasn't able to process your request."


# Create a singleton instance to be imported elsewhere
event_recommendation_graph = EventRecommendationGraph()