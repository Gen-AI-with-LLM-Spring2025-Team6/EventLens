import json
import os
import sys
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from langgraph.graph import StateGraph, START, END
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from fastapi_backend.eventlens_agent.state import State
from fastapi_backend.eventlens_agent.tools.rag_data_retreival_api import retrieve_events
from fastapi_backend.eventlens_agent.tools.weather_api import check_weather_for_address, process_weather_query
from fastapi_backend.eventlens_agent.tools.maps_api import get_directions, process_directions_query
from fastapi_backend.eventlens_agent.tools.sentiment_serpapi import format_reviews_for_llm, process_sentiment_query

class EventRecommendationGraph:
    """
    A complete event recommendation system using LangGraph.
    """
    
    def __init__(self, llm_model: str = "gpt-4o"):
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
                "func": process_weather_query,
                "description": "Checks weather forecast for a specific location and date"
            },
            "get_directions": {
                "func": process_directions_query,
                "description": "Gets directions and travel information between two locations"
            },
            "get_event_reviews": {
                "func": process_sentiment_query,
                "description": "Gets reviews and sentiment about an event"
            }
        }
        
        # Legacy function mappings for backward compatibility
        self.legacy_functions = {
            "check_weather": check_weather_for_address,
            "get_directions": get_directions,
            "get_event_reviews": format_reviews_for_llm
        }
        
        # Create the graph
        self.graph = self._build_graph()
    
    def _is_relevant_query(self, query: str, conversation_history: Optional[List] = None) -> bool:
        """
        Determine if a query is related to events in Boston, considering conversation history.
        
        Args:
            query: The user's query
            conversation_history: Optional conversation history to provide context
                
        Returns:
            bool: True if the query is relevant, False otherwise
        """
        # Use the LLM to classify whether the query is related to events
        system_content = """
        You are a query classifier for EventLens, an event recommendation system focused on Boston events.
        Determine if the query is related ONLY to:
        1. Events in Boston (concerts, sports, festivals, etc.)
        2. Weather specifically for attending events
        3. Directions to event venues
        4. Reviews of Boston events
        5. Follow-up questions about events mentioned earlier
        
        Consider the FULL CONVERSATION HISTORY when determining relevance.
        A query that seems unrelated when viewed alone might be a follow-up
        to previous event-related discussion.
        
        Return ONLY "relevant" or "not_relevant" with no additional text.
        
        Examples of RELEVANT queries:
        - "What events are happening in Boston this weekend?"
        - "Are there any jazz concerts in Boston?"
        - "How's the weather for the Red Sox game tomorrow?"
        - "How do I get to Symphony Hall?"
        - "How is the weather during the event?"
        - "What do people think about the Boston Marathon?"
        - "What are you? / What kind of events do you cover?"
        - "Based on the weather which mode of transit would you suggest?"
        - "How long will it take to get there?" (when previously discussing an event)
        - "Is parking available?" (when previously discussing an event venue)
        - "What time does it start?" (when referring to an event mentioned earlier)
        
        Examples of NOT RELEVANT queries:
        - "What is the capital of France?"
        - "Who is the President of the United States?"
        - "Explain quantum computing"
        - "What are the latest stock prices?"
        - "Tell me about the history of Rome"
        - "How's the weather in Paris?" (not event-related)
        - "What's the distance from New York to Los Angeles?" (not Boston-related)
        """
        
        system_message = SystemMessage(content=system_content)
        
        # Create user message, potentially including conversation history for context
        if conversation_history and len(conversation_history) > 0:
            # Format conversation history for context
            history_text = ""
            for i, msg in enumerate(conversation_history):
                if isinstance(msg, HumanMessage):
                    history_text += f"Human: {msg.content}\n"
                elif isinstance(msg, AIMessage):
                    history_text += f"Assistant: {msg.content}\n"
            
            # Create a user message with context
            user_content = f"""Conversation history:
{history_text}

Current query: {query}

Is this query relevant to Boston events, considering the conversation history?"""
            user_message = HumanMessage(content=user_content)
        else:
            # No conversation history, just check the current query
            user_message = HumanMessage(content=query)
        
        try:
            response = self.llm.invoke([system_message, user_message])
            return response.content.strip().lower() == "relevant"
        except:
            # If there's an error, default to rejecting the query
            return False
    
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
        graph_builder.add_node("answer_generator", self._final_answer_node)
        
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
                "answer_generator": "answer_generator",
                None: END  # If no more tools needed and no final answer needed
            }
        )
        
        # Each tool routes back to controller to decide next step
        graph_builder.add_edge("rag_tool", "controller")
        graph_builder.add_edge("weather_tool", "controller")
        graph_builder.add_edge("maps_tool", "controller")
        graph_builder.add_edge("reviews_tool", "controller")
        
        # Final answer -> END
        graph_builder.add_edge("answer_generator", END)
        
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

        # Improved system content with stronger guidance for event queries
        system_content = """You are EventLens, an event recommendation assistant for Boston. 
        Your job is to determine which tools to use based on the user's query.
        You have the following tools available:

        - retrieve_events: Finds relevant events based on user interests and preferences from the database
        - check_weather: Checks weather forecast for a specific location and date
        - get_directions: Gets directions and travel information between two locations
        - get_event_reviews: Gets reviews and sentiment about an event

        IMPORTANT RULES:
        1. If the user asks about ANY kind of events (sports, music, family, etc.) or activities in Boston, 
           ALWAYS include "retrieve_events" in your tools selection.
        
        2. For queries like "suggest events", "what's happening", "find activities", "show me events", 
           or similar requests for event recommendations, ALWAYS use the "retrieve_events" tool FIRST.
           
        3. If the user asks about weather for an event, include both "retrieve_events" and "check_weather".
        
        4. If the user asks about directions or how to get to an event, include both "retrieve_events" 
           and "get_directions".
           
        5. If the user asks about reviews or opinions about an event, include both "retrieve_events" 
           and "get_event_reviews".

        Analyze the user's query and determine which tools are needed in what order.
        Return a JSON list of tool names in the order they should be used.
        Only include tools that are necessary for the user's query.
        
        Consider the full conversation history when determining which tools to use. 
        For example, if a user asks about directions to an event mentioned earlier, 
        you should use the get_directions tool.
        """

        system_message = SystemMessage(content=system_content)
        
        # Create a message list including conversation history
        # This is key for maintaining context between turns
        conversation_messages = []
        for msg in messages:
            conversation_messages.append(msg)
            
        # Add the system message at the beginning
        message_list = [system_message] + conversation_messages
        
        # Use message list directly with all conversation history
        response = self.llm.invoke(message_list)

        # Rest of the method remains the same
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

            # Force inclusion of retrieve_events for event-related queries
            event_keywords = ["event", "happening", "activity", "show me", "suggest", "sports", 
                            "concert", "game", "festival", "show", "performance"]
            if any(term in last_user_message.lower() for term in event_keywords):
                if "retrieve_events" not in tools_to_call:
                    tools_to_call.insert(0, "retrieve_events")  # Add at the beginning

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
        This method ensures all tools in tools_to_call are called before moving to final answer.
        """
        tools_to_call = state.get("tools_to_call", [])
        tools_called = state.get("tools_called", [])
        
        # Get any remaining tools that need to be called
        next_tools = [t for t in tools_to_call if t not in tools_called]
        
        if not next_tools:
            # All tools have been called, now we can generate the final answer
            return {"need_final_answer": True}
        else:
            # There are still tools that need to be called
            # Set the current tool to the next one in the list
            return {"current_tool": next_tools[0]}
    
    def _route_to_next_tool(self, state: State) -> str:
        """
        Determine the next node to route to based on the current state.
        """
        # If we need a final answer, route to final answer node
        if state.get("need_final_answer"):
            return "answer_generator"
        
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
            return "answer_generator"
    
    def _rag_tool_node(self, state: State) -> Dict:
        """
        Execute the RAG tool to retrieve relevant events.
        """
        current_tool = state.get("current_tool")
        if current_tool != "retrieve_events":
            return {}
        
        # Get all messages to provide context
        messages = state["messages"]
        
        # Extract context from previous assistant messages if they exist
        context = ""
        previous_results = state.get("tool_results", {})
        if "retrieve_events" in previous_results:
            context += f"Previously mentioned events: {previous_results['retrieve_events']}\n\n"
        
        # Get the latest user query
        last_user_message = None
        for msg in reversed(messages):
            if isinstance(msg, HumanMessage):
                last_user_message = msg.content
                break
        
        if not last_user_message:
            return {}
        
        # Add context to help with understanding references to previous events
        enhanced_query = last_user_message
        if context:
            # For LLM processing, we'll just pass the raw user message
            # The context will be used by retrieve_events internally through the state
            pass
        
        # Execute the RAG tool with context
        try:
            result = retrieve_events(enhanced_query)
            
            # Update the state with whatever the RAG returned
            tools_called = state.get("tools_called", [])
            tools_called.append("retrieve_events")
            
            tool_results = state.get("tool_results", {})
            tool_results["retrieve_events"] = result
            
            return {
                "tools_called": tools_called,
                "tool_results": tool_results
            }
        except Exception as e:
            # If there's an exception, just return the error message
            error_message = f"Error retrieving event information: {str(e)}. Please try again or modify your query."
            
            tools_called = state.get("tools_called", [])
            tools_called.append("retrieve_events")
            
            tool_results = state.get("tool_results", {})
            tool_results["retrieve_events"] = error_message
            
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
        
        # Get all user messages to provide context
        messages = state["messages"]
        
        # Get the latest user query
        last_user_message = None
        for msg in reversed(messages):
            if isinstance(msg, HumanMessage):
                last_user_message = msg.content
                break
        
        if not last_user_message:
            return {}
            
        # Extract location context from previous results if available
        previous_results = state.get("tool_results", {})
        event_context = previous_results.get("retrieve_events", "")
        
        # If we have previous event results, add them as context to the query
        enhanced_query = last_user_message
        if event_context:
            # Add event context to the query to help extract better parameters
            # Use the natural language processing method that can handle full context
            pass
        
        # Use the direct natural language processing method - wait for complete results
        result = process_weather_query(enhanced_query)
        
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
        
        # Get all user messages to provide context
        messages = state["messages"]
        
        # Extract event location context from previous results
        previous_results = state.get("tool_results", {})
        event_context = previous_results.get("retrieve_events", "")
        
        # Get the latest user query
        last_user_message = None
        for msg in reversed(messages):
            if isinstance(msg, HumanMessage):
                last_user_message = msg.content
                break
        
        if not last_user_message:
            return {}
        
        # If we have previous event results, create an enhanced query
        enhanced_query = last_user_message
        if event_context:
            # Extract location information from previous event results
            location_info = self._extract_location_from_results(event_context)
            if location_info:
                enhanced_query = f"{last_user_message} (regarding the location at {location_info})"
        
        # Use the direct natural language processing method - wait for complete results
        result = process_directions_query(enhanced_query)
        
        # Update the state
        tools_called = state.get("tools_called", [])
        tools_called.append("get_directions")
        
        tool_results = state.get("tool_results", {})
        tool_results["get_directions"] = result
        
        return {
            "tools_called": tools_called,
            "tool_results": tool_results
        }
    
    def _extract_location_from_results(self, event_results: str) -> Optional[str]:
        """
        Extract location information from event results.
        
        Args:
            event_results: String containing event information
            
        Returns:
            Location string if found, None otherwise
        """
        # Use LLM to extract location from the event results
        system_message = SystemMessage(content="""
        Extract the location or address of the event mentioned in the text.
        Return only the location or address, with no additional text.
        If no location is mentioned, return NONE.
        """)
        
        user_message = HumanMessage(content=event_results)
        
        # Get location from LLM
        response = self.llm.invoke([system_message, user_message])
        
        location = response.content.strip()
        if location.upper() == "NONE":
            return None
            
        return location
    
    def _reviews_tool_node(self, state: State) -> Dict:
        """
        Execute the reviews tool to get event reviews.
        """
        current_tool = state.get("current_tool")
        if current_tool != "get_event_reviews":
            return {}
        
        # Get all user messages for context
        messages = state["messages"]
        
        # Extract event name context from previous results
        previous_results = state.get("tool_results", {})
        event_context = previous_results.get("retrieve_events", "")
        
        # Get the latest user query
        last_user_message = None
        for msg in reversed(messages):
            if isinstance(msg, HumanMessage):
                last_user_message = msg.content
                break
        
        if not last_user_message:
            return {}
        
        # If we have previous event results, create an enhanced query
        enhanced_query = last_user_message
        if event_context:
            # Extract event name information from previous event results
            event_name = self._extract_event_name_from_results(event_context)
            if event_name:
                enhanced_query = f"{last_user_message} (regarding {event_name})"
        
        # Use the direct natural language processing method - wait for complete results
        result = process_sentiment_query(enhanced_query)
        
        # Update the state
        tools_called = state.get("tools_called", [])
        tools_called.append("get_event_reviews")
        
        tool_results = state.get("tool_results", {})
        tool_results["get_event_reviews"] = result
        
        return {
            "tools_called": tools_called,
            "tool_results": tool_results
        }
    
    def _extract_event_name_from_results(self, event_results: str) -> Optional[str]:
        """
        Extract event name information from event results.
        
        Args:
            event_results: String containing event information
            
        Returns:
            Event name string if found, None otherwise
        """
        # Use LLM to extract event name from the event results
        system_message = SystemMessage(content="""
        Extract the name of the event mentioned in the text.
        Return only the event name, with no additional text.
        If no event is mentioned, return NONE.
        """)
        
        user_message = HumanMessage(content=event_results)
        
        # Get event name from LLM
        response = self.llm.invoke([system_message, user_message])
        
        event_name = response.content.strip()
        if event_name.upper() == "NONE":
            return None
            
        return event_name
    
    def _final_answer_node(self, state: State) -> Dict:
        """
        Generate the final answer based on the results from all tools.
        """
        messages = state["messages"]
        tool_results = state.get("tool_results", {})
        tools_called = state.get("tools_called", [])
        
        # Create direct messages instead of using ChatPromptTemplate
        system_content = f"""You are EventLens, an AI assistant specifically designed to help with events in Boston.
        ALWAYS identify yourself as EventLens if asked about your identity or name.
        
        IMPORTANT: You are EventLens, an event assistant created specifically for Boston events and activities.
        If asked who you are, your name, or about your creator, clearly state that you are EventLens, 
        an AI event recommendation assistant focused on Boston events.
        
        Synthesize the results from the tools into a coherent, helpful response for the user.
        Use all the available information to provide a complete response.
        Be conversational and friendly.
        
        IMPORTANT: NEVER apologize for delays or mention that you were processing or retrieving information.
        Never use phrases like "Let me check", "I'm gathering", "Please wait", or "I apologize for the delay".
        
        CRUCIAL: If the retrieve_events tool returned an error or no results, clearly state that you 
        couldn't retrieve current event information and suggest that the user try again later or 
        check Boston event websites directly.
        
        You have results from the following tools:
        {", ".join(tools_called)}
        
        Include specific details from the tools' results when available:
        - Event details from the retrieve_events tool
        - Weather information from the check_weather tool
        - Directions and travel info from the get_directions tool
        - Reviews and sentiment from the get_event_reviews tool
        
        Only mention tools that were actually used.
        
        Make your response conversational and maintain context from the entire conversation.
        If the user is referring to events or locations mentioned earlier, acknowledge that in your response.
        
        Tool results: {json.dumps(tool_results)}
        """
        
        system_message = SystemMessage(content=system_content)
        
        # Include the full conversation history
        response = self.llm.invoke([system_message] + messages)
        
        # Return the final answer
        return {"messages": messages + [AIMessage(content=response.content)]}
    
    def invoke(self, message: str, conversation_history: Optional[List] = None) -> Tuple[str, List]:
        """
        Invoke the event recommendation system with a user message.
        
        Args:
            message: The user's message.
            conversation_history: Optional previous conversation history.
            
        Returns:
            Tuple[str, List]: The assistant's response and updated conversation history.
        """
        # Check if the query is related to events in Boston, considering conversation history
        if not self._is_relevant_query(message, conversation_history):
            off_topic_response = """I'm EventLens, your Boston events assistant. I'm designed to help you find events, 
            get weather information for events, directions to venues, and reviews of events in Boston.
            
            I can't help with general knowledge questions or topics unrelated to Boston events.
            
            Could you please ask me about events, activities, or venues in Boston instead?"""
            
            # Create message objects
            human_msg = HumanMessage(content=message)
            ai_msg = AIMessage(content=off_topic_response)
            
            # Update conversation history
            if conversation_history is None:
                new_history = [human_msg, ai_msg]
            else:
                new_history = conversation_history + [human_msg, ai_msg]
                
            return off_topic_response, new_history
        
        # For relevant queries, proceed with normal processing
        if conversation_history is None:
            # Start a new conversation
            state = {
                "messages": [HumanMessage(content=message)]
            }
        else:
            # Continue an existing conversation
            state = {
                "messages": conversation_history + [HumanMessage(content=message)]
            }
        
        # Run the graph - wait for complete processing before returning
        result = self.graph.invoke(state)
        
        # Return the last message from the assistant
        messages = result.get("messages", [])
        if messages:
            # Return both the message content and the updated conversation history
            return messages[-1].content, messages
        
        return "I'm sorry, I wasn't able to process your request.", state.get("messages", [])
    
    def chat(self):
        """
        Interactive chat session with conversation memory.
        """
        print("="*80)
        print("Welcome to EventLens! Ask me about events, weather, directions, or reviews.")
        print("Type 'exit' or 'quit' to end the session.")
        print("="*80)
        
        # Initialize conversation history
        conversation_history = []
        
        while True:
            user_input = input("\nYou: ")
            
            if user_input.lower() in ["exit", "quit"]:
                print("\nThank you for using EventLens. Goodbye!")
                break
            
            # Process the message with conversation history - NO INTERMEDIATE RESPONSES
            # Don't show any response until processing is complete
            response, conversation_history = self.invoke(user_input, conversation_history)
            
            # Only print the final complete response
            print(f"\nEventLens: {response}")


# Create a singleton instance to be imported elsewhere
event_recommendation_graph = EventRecommendationGraph()

# Add a standalone function for easy use
def process_query(message: str, conversation_history=None):
    """Standalone function to process a query with the graph."""
    response_text, updated_history = event_recommendation_graph.invoke(message, conversation_history)
    return response_text, updated_history