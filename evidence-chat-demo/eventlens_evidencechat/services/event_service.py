import re
from database.snowflake_connector import vector_search_snowflake
from llm.openai_client import generate_openai_response

def analyze_user_query(question, api_key, model_name):
    """
    Generate chain-of-thought reasoning steps about what kinds of events to search for.
    
    Args:
        question (str): User's event search query
        api_key (str): OpenAI API key
        model_name (str): Name of the OpenAI model to use
        
    Returns:
        list: List of specific search queries derived from the user question
    """
    system_prompt = (
        "You are EventLens, an intelligent event recommendation assistant. Given a user's request, "
        "break down their preferences into specific, searchable aspects of events. "
        "For example, if they ask for 'family-friendly weekend activities in Boston', break this down into "
        "separate components like 'family-friendly events', 'weekend events', 'activities', and 'Boston location'. "
        "Provide 3-5 specific, diverse search queries that would help find relevant events. "
        "Output ONLY the numbered search queries without additional explanation."
    )
    user_prompt = f"User request: {question}\n\nBreak down this request into specific search queries for finding events:"
    
    output = generate_openai_response(
        system_prompt, user_prompt, model_name, api_key, max_tokens=300, temperature=0.3
    )
    
    if output:
        # Extract numbered steps
        search_queries = []
        for line in output.split('\n'):
            line = line.strip()
            if re.match(r'^\d+\.', line):
                query = re.sub(r'^\d+\.\s*', '', line)
                search_queries.append(query)
        
        return search_queries
    else:
        return []

def search_events_per_query(search_queries, conn, table_name, embedding_col, text_col, limit_per_query=3):
    """
    Search for relevant events using each search query.
    
    Args:
        search_queries (list): List of search queries
        conn: Snowflake connection
        table_name (str): Name of the table containing event data
        embedding_col (str): Name of the column containing embeddings
        text_col (str): Name of the column containing event text
        limit_per_query (int): Maximum number of results per query
        
    Returns:
        tuple: (all_results, query_results_map)
            all_results: List of (event_id, text, similarity) tuples
            query_results_map: Dictionary mapping queries to their results
    """
    all_results = []
    query_results_map = {}  # Keep track of which query returned which results
    seen_texts = set()  # Track unique texts to avoid duplicates
    
    for query in search_queries:
        results = vector_search_snowflake(
            query, conn, table_name, embedding_col, text_col, limit=limit_per_query
        )
        
        # Store results by query for display purposes
        query_results_map[query] = []
        
        # Add only unique results to the final list
        for result in results:
            event_id = result[0]
            text = result[1]
            similarity = result[2]
            
            if text not in seen_texts:
                all_results.append((event_id, text, similarity))
                query_results_map[query].append((event_id, text, similarity))
                seen_texts.add(text)
    
    # Sort by similarity score (descending)
    all_results.sort(key=lambda x: x[2], reverse=True)
    
    return all_results, query_results_map

def generate_event_recommendations(original_question, search_queries, event_results, api_key, model_name):
    """
    Generate personalized event recommendations based on search queries and retrieved events.
    
    Args:
        original_question (str): User's original question
        search_queries (list): List of derived search queries
        event_results (list): List of (event_id, text, similarity) tuples
        api_key (str): OpenAI API key
        model_name (str): Name of the OpenAI model to use
        
    Returns:
        str: Formatted event recommendations
    """
    # Extract event texts and IDs from results
    event_texts = [row[1] for row in event_results]
    event_ids = [row[0] for row in event_results]
    
    # Combine event text with its ID for context
    event_context = []
    for i, (event_id, text) in enumerate(zip(event_ids, event_texts)):
        event_context.append(f"EVENT ID: {event_id}\n{text}")
    
    context = "\n\n".join(event_context)
    
    # Format search queries
    queries_text = "\n".join([f"{i+1}. {query}" for i, query in enumerate(search_queries)])
    
    system_prompt = (
        "You are EventLens, an intelligent event recommendation assistant. Based on the user's original request, "
        "the search queries we derived, and the events we found, provide personalized event recommendations. "
        "Highlight key details like event names, dates, locations, and why these events match the user's preferences. "
        "Your recommendations should be enthusiastic but honest, focusing only on events mentioned in the provided context. "
        "IMPORTANT: For each event recommendation, include its EVENT ID in your response so the user can reference it later."
    )
    user_prompt = (
        f"User's original request: {original_question}\n\n"
        f"Search queries we used:\n{queries_text}\n\n"
        f"Events found from database:\n{context}\n\n"
        f"Please provide personalized event recommendations that match the user's interests, "
        f"grouping them by the type of events or search criteria they satisfy. "
        f"Make sure to include each event's EVENT ID in your recommendations."
    )
    
    return generate_openai_response(
        system_prompt, user_prompt, model_name, api_key, max_tokens=500, temperature=0.3
    ) or "Sorry, I couldn't find any events matching your preferences. Please try a different request or broaden your criteria."