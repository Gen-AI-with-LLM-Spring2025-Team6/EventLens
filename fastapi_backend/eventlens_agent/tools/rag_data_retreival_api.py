import os
import re
import json
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage

# Import database connection utilities
from db_connection import snowflake_connection, close_connection
from snowflake_queries import vector_search_snowflake

# Load environment variables
load_dotenv()

# Get API keys from environment variables
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Check if API key is available
if not OPENAI_API_KEY:
    raise ValueError("OpenAI API key is required. Set the OPENAI_API_KEY environment variable.")


def analyze_user_query(user_query: str) -> List[str]:
    """
    Analyze user query and break it down into more specific search queries.
    
    Args:
        user_query: The user's natural language query about events
        
    Returns:
        List of refined search queries
    """
    # Initialize LLM with GPT-4o model
    llm = ChatOpenAI(model="gpt-4o", api_key=OPENAI_API_KEY)
    
    # Create system and user messages
    system_message = SystemMessage(content="""You are EventLens, an intelligent event assistant. 
    Break down the user's request into 5 diverse and specific event search queries. 
    These queries should cover different interpretations and aspects of the user's request.
    
    For example, if the user asks "What's happening this weekend?", your queries might be:
    1. Cultural events in Boston this weekend
    2. Family-friendly activities in Boston this weekend
    3. Music concerts and performances in Boston this weekend
    4. Outdoor activities and festivals in Boston this weekend
    5. Sports events in Boston this weekend
    
    Return ONLY the numbered list of queries, nothing else.
    """)
    
    user_message = HumanMessage(content=f"User query: {user_query}\n\nReturn 5 search queries:")
    
    # Get the response from the LLM
    response = llm.invoke([system_message, user_message])
    
    # Extract the numbered queries from the response
    search_queries = []
    for line in response.content.split("\n"):
        if match := re.match(r"^\d+\.\s*(.+)", line.strip()):
            search_queries.append(match.group(1))
    
    return search_queries


def fetch_event_context(search_queries: List[str], table_name: str = "EVENTLENS_DB.EDW.FACT_EVENTS_DETAILS", 
                       embedding_col: str = "VECTOR_EMBEDDING", text_col: str = "STRUCTURED_TEXT", 
                       limit_per_query: int = 5) -> List[tuple]:
    """
    Fetch events from Snowflake database using vector search.
    
    Args:
        search_queries: List of search queries
        table_name: Name of the Snowflake table containing events
        embedding_col: Name of the column containing vector embeddings
        text_col: Name of the column containing event text
        limit_per_query: Maximum number of results per query
        
    Returns:
        List of (event_id, text, similarity) tuples
    """
    conn = snowflake_connection()
    results = []
    seen_ids = set()
    
    try:
        for query in search_queries:
            # Use the provided vector_search_snowflake function
            partial_results = vector_search_snowflake(
                query=query,
                conn=conn,
                table_name=table_name,
                embedding_col=embedding_col,
                text_col=text_col,
                limit=limit_per_query
            )
            
            # Add non-duplicate results to the list
            for event_id, text, sim in partial_results:
                if event_id not in seen_ids:
                    results.append((event_id, text, sim))
                    seen_ids.add(event_id)
        return results
    
    finally:
        # Always close the connection
        close_connection(conn)


def refine_event_ids_from_context(event_results: List[tuple], search_queries: List[str], 
                                original_question: str, limit: int = 10) -> List[int]:
    """
    Select the best event IDs from the search results using LLM.
    
    Args:
        event_results: List of (event_id, text, similarity) tuples
        search_queries: List of search queries used
        original_question: Original user query
        limit: Maximum number of event IDs to return
        
    Returns:
        List of event IDs
    """
    # If we have fewer results than the limit, return all IDs
    if len(event_results) <= limit:
        return [eid for eid, _, _ in event_results]
    
    # Initialize LLM with GPT-4o model
    llm = ChatOpenAI(model="gpt-4o", api_key=OPENAI_API_KEY)
    
    # Format context from results
    context = "\n\n".join([f"EVENT_ID: {eid}\n{text}" for eid, text, _ in event_results])
    
    # Format query list
    query_list = "\n".join([f"{i+1}. {q}" for i, q in enumerate(search_queries)])
    
    # Create system and user messages
    system_message = SystemMessage(content=f"""You are EventLens, an intelligent event recommendation assistant. 
    Based on the user's request, search queries, and event context, select the best {limit} unique EVENT_IDs. 
    Choose events that best match the user's intent and provide a diverse set of options.
    
    Return only a JSON array of the EVENT_IDs as integers, with no other text.
    """)
    
    user_message = HumanMessage(content=f"""Original Query: {original_question}

Derived Queries:
{query_list}

Event Context:
{context}

Return {limit} best matching EVENT_IDs as a JSON array of integers:""")
    
    # Get the response from the LLM
    response = llm.invoke([system_message, user_message])
    
    try:
        # Parse the JSON response
        event_ids = json.loads(response.content)
        
        # Ensure all IDs are integers
        event_ids = [int(eid) for eid in event_ids]
        
        # Ensure uniqueness and limit to specified number
        return list(dict.fromkeys(event_ids))[:limit]
    
    except (json.JSONDecodeError, ValueError):
        # Fallback if parsing fails: return top N IDs sorted by similarity
        event_results.sort(key=lambda x: x[2], reverse=True)
        return [int(eid) for eid, _, _ in event_results[:limit]]


def fetch_event_details_from_ids(event_ids: List[int]) -> List[Dict[str, Any]]:
    """
    Fetch detailed event information from the database based on event IDs.
    
    Args:
        event_ids: List of event IDs
        
    Returns:
        List of event dictionaries with full details
    """
    if not event_ids:
        return []

    # Create SQL placeholders for the IDs
    placeholders = ", ".join(["%s"] * len(event_ids))
    
    # SQL query to get event details
    query = f"""
        SELECT EVENT_ID, EVENT_TITLE, START_DATE, END_DATE, START_TIME, END_TIME,
               OCCURRENCES, FULL_ADDRESS, LOCATION, ADMISSION, DESCRIPTION,
               EVENT_URL, IMAGE_S3_URL, CATEGORIES
        FROM EVENTLENS_DB.EDW.FACT_EVENTS_DETAILS
        WHERE EVENT_ID IN ({placeholders})
    """

    conn = snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query, tuple(event_ids))
        
        # Get column names from cursor description
        columns = [col[0] for col in cursor.description]
        
        # Fetch all rows and convert to dictionaries
        events = []
        for row in cursor.fetchall():
            event = {}
            for i, col in enumerate(columns):
                event[col] = row[i]
            events.append(event)
        
        return events
    
    finally:
        close_connection(conn)


def format_event_recommendations(events: List[Dict[str, Any]], user_query: str) -> str:
    """
    Format event recommendations into a conversational response.
    
    Args:
        events: List of event dictionaries
        user_query: Original user query
        
    Returns:
        Formatted event recommendations as text
    """
    if not events:
        return "I couldn't find any events matching your query. Would you like to try a different search?"
    
    # Initialize LLM with GPT-4o model
    llm = ChatOpenAI(model="gpt-4o", api_key=OPENAI_API_KEY)
    
    # Format events for the prompt
    events_json = json.dumps(events, indent=2)
    
    # Create system and user messages
    system_message = SystemMessage(content="""You are EventLens, an intelligent event recommendation assistant.
    Create a conversational response about the events that best match the user's query.
    
    Your response should:
    1. Acknowledge the user's request
    2. Group similar events when appropriate
    3. Highlight key details (date, time, location, cost)
    4. Be engaging and helpful
    5. Include a brief suggestion for what might be most interesting based on the query
    
    Focus on the most relevant details rather than listing everything about each event.
    """)
    
    user_message = HumanMessage(content=f"""User Query: {user_query}

Event Information:
{events_json}

Provide recommendations based on these events:""")
    
    # Get the response from the LLM
    response = llm.invoke([system_message, user_message])
    
    return response.content


def retrieve_events(user_query: str) -> str:
    """
    Main function that processes a user query and returns event recommendations.
    This is the function that will be called from the graph.py system.
    
    Args:
        user_query: User's natural language query about events
        
    Returns:
        Formatted event recommendations as text
    """
    try:
        # Step 1: Analyze the user query to generate search queries
        search_queries = analyze_user_query(user_query)
        
        # Step 2: Fetch event context using vector search
        event_results = fetch_event_context(
            search_queries=search_queries,
            table_name="EVENTLENS_DB.EDW.FACT_EVENTS_DETAILS",
            embedding_col="VECTOR_EMBEDDING",
            text_col="STRUCTURED_TEXT",
            limit_per_query=5
        )
        
        if not event_results:
            return "I couldn't find any events matching your criteria. Would you like to try a different search?"
        
        # Step 3: Refine the event IDs based on the context
        selected_ids = refine_event_ids_from_context(
            event_results=event_results,
            search_queries=search_queries,
            original_question=user_query,
            limit=10
        )
        
        # Step 4: Fetch the complete details for the selected events
        event_details = fetch_event_details_from_ids(selected_ids)
        
        # Step 5: Format the event recommendations
        return format_event_recommendations(event_details, user_query)
    
    except Exception as e:
        return f"I'm sorry, I encountered an error while searching for events: {str(e)}. Please try again or modify your query."


## For testing the API directly
#if __name__ == "__main__":
#    # Test with different queries
#    test_queries = [
#        "What's happening in Boston this weekend?"
#    ]
#    
#    for query in test_queries:
#        print(f"\nTEST QUERY: {query}")
#        print("-" * 50)
#        print(retrieve_events(query))
#        print("=" * 80)