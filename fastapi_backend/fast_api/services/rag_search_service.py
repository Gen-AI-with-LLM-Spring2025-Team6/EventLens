import os
import re
import pandas as pd
from openai import OpenAI
import json
from fastapi_backend.fast_api.config.db_connection import snowflake_connection, close_connection
from fastapi_backend.fast_api.utils.snowflake_queries import vector_search_snowflake

def generate_openai_response(system_prompt, user_prompt, model_name, api_key, max_tokens=500, temperature=0.3):
    client = OpenAI(api_key=api_key)
    try:
        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=temperature,
            max_tokens=max_tokens
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"OpenAI Error: {e}"

def analyze_user_query(user_query, model_name, api_key):
    system_prompt = (
        "You are EventLens, an intelligent event assistant. Break down the user's request "
        "into 5 diverse and specific event search queries. Return only the numbered list."
    )
    user_prompt = f"User query: {user_query}\n\nReturn 5 search queries:"
    output = generate_openai_response(system_prompt, user_prompt, model_name, api_key)
    
    search_queries = []
    for line in output.split("\n"):
        if match := re.match(r"^\d+\.\s*(.+)", line.strip()):
            search_queries.append(match.group(1))
    return search_queries

def fetch_event_context(search_queries, table_name, embedding_col, text_col, limit_per_query=5):
    conn = snowflake_connection()
    results = []
    seen_ids = set()
    try:
        for query in search_queries:
            partial_results = vector_search_snowflake(
                query=query,
                conn=conn,
                table_name=table_name,
                embedding_col=embedding_col,
                text_col=text_col,
                limit=limit_per_query
            )
            for event_id, text, sim in partial_results:
                if event_id not in seen_ids:
                    results.append((event_id, text, sim))
                    seen_ids.add(event_id)
        return results
    finally:
        close_connection(conn)

def refine_event_ids_from_context(event_results, search_queries, original_question, model_name, api_key):
    context = "\n\n".join([f"EVENT_ID: {eid}\n{text}" for eid, text, _ in event_results])
    query_list = "\n".join([f"{i+1}. {q}" for i, q in enumerate(search_queries)])
    
    system_prompt = (
        "You are EventLens, an intelligent event recommendation assistant. Based on the user's request, "
        "queries used, and event context, select the best 12 unique EVENT_IDs. "
        "Return only the list of EVENT_IDs, one per line, prefixed with the number."
    )
    user_prompt = (
        f"Original Query: {original_question}\n\n"
        f"Derived Queries:\n{query_list}\n\n"
        f"Event Context:\n{context}\n\n"
        f"Return 12 best matching EVENT_IDs (no duplicates):"
    )

    output = generate_openai_response(system_prompt, user_prompt, model_name, api_key)
    ids = []
    for line in output.splitlines():
        if match := re.match(r"^\d+\.\s*(\d+)", line.strip()):
            ids.append(int(match.group(1)))
    return list(dict.fromkeys(ids))[:12]  # Ensure uniqueness, limit to 12

def fetch_event_details_from_ids(event_ids):
    if not event_ids:
        return []

    placeholders = ", ".join(["%s"] * len(event_ids))
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
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)
        return df.to_dict(orient="records")
    finally:
        close_connection(conn)


def clean_event_text_with_openai(events, model_name, api_key):
    """
    Send raw event data to OpenAI for formatting and cleanup before sending to frontend.

    Args:
        events (list): List of event dicts
        model_name (str): OpenAI model name
        api_key (str): OpenAI API key

    Returns:
        list: Cleaned event dicts
    """
    system_prompt = (
        "You are EventLens, an assistant helping improve the presentation of event information. "
        "You will receive a list of events, each with fields like title, description, admission, and address. "
        "For each event, clean up the DESCRIPTION and other text fields to make them grammatically correct, clear, "
        "and readable in a user interface. "
        "Do NOT change or touch the EVENT_URL or IMAGE_S3_URL fields. "
        "Return only the cleaned list of events in JSON format."
    )

    user_prompt = f"Here is the list of events:\n\n{json.dumps(events, indent=2)}"

    cleaned_json_str = generate_openai_response(
        system_prompt, user_prompt, model_name, api_key, max_tokens=3000, temperature=0.3
    )

    try:
        cleaned_data = json.loads(cleaned_json_str)
        return cleaned_data if isinstance(cleaned_data, list) else events
    except Exception:
        return events  # fallback to raw if parsing fails



def rag_event_search_pipeline(user_query, model_name, api_key):
    queries = analyze_user_query(user_query, model_name, api_key)
    context_results = fetch_event_context(
        search_queries=queries,
        table_name="EVENTLENS_DB.EDW.FACT_EVENTS_DETAILS",
        embedding_col="VECTOR_EMBEDDING",
        text_col="STRUCTURED_TEXT",
        limit_per_query=5
    )
    selected_ids = refine_event_ids_from_context(
        context_results,
        queries,
        original_question=user_query,
        model_name=model_name,
        api_key=api_key
    )
    raw_events = fetch_event_details_from_ids(selected_ids)
    return clean_event_text_with_openai(raw_events, model_name, api_key)

