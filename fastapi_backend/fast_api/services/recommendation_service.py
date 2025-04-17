import json
import pandas as pd
from typing import List, Dict, Any
from fastapi_backend.fast_api.config.db_connection import snowflake_connection, close_connection
from fastapi_backend.fast_api.utils.open_ai_calls import generate_openai_response


def get_event_ids_from_cortex_preview(cursor, query_text: str, limit: int = 5) -> List[tuple]:
    cortex_query = f"""
    SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'EVENTLENS_DB.EDW.EVENTS_SEARCH',
        '{{ "query": "{query_text}", "limit": {limit}, "columns": ["EVENT_ID", "STRUCTURED_TEXT"] }}'
    ) AS result
    """
    cursor.execute(cortex_query)
    rows = cursor.fetchall()

    event_info = []
    for row in rows:
        parsed = json.loads(row[0])
        if isinstance(parsed, dict) and "results" in parsed:
            for entry in parsed["results"]:
                event_id = entry.get("EVENT_ID")
                text = entry.get("STRUCTURED_TEXT")
                if event_id and text:
                    event_info.append((event_id, text))
    return event_info


def fetch_recommended_events(user_id: int) -> List[Dict[str, Any]]:
    conn = None
    cursor = None

    try:
        conn = snowflake_connection()
        cursor = conn.cursor()

        # Step 1: Get user interests
        cursor.execute(
            "SELECT INTERESTS FROM EVENTLENS_DB.EDW.USERDETAILS WHERE USER_ID = %s",
            (user_id,)
        )
        interest_row = cursor.fetchone()
        if not interest_row:
            return []
        user_interests = interest_row[0]

        # Step 2: Get interest-based events
        interest_context = get_event_ids_from_cortex_preview(cursor, user_interests, limit=5)

        # Step 3: Get recent search history
        cursor.execute("""
            SELECT DISTINCT SEARCH_QUERY
                FROM (
                    SELECT SEARCH_QUERY, TIMESTAMP
                    FROM EVENTLENS_DB.EDW.USER_SEARCH_HISTORY
                    WHERE USER_ID = %s
                    ORDER BY TIMESTAMP DESC
                )
                LIMIT 5;
        """, (user_id,))
        search_rows = cursor.fetchall()
        search_queries = [row[0] for row in search_rows]

        # Step 4: Search events based on search history
        search_context = []
        for query_text in search_queries:
            result = get_event_ids_from_cortex_preview(cursor, query_text, limit=2)
            search_context.extend(result)

        # Step 5: Prepare combined context
        def format_context(label: str, context: List[tuple]) -> str:
            return f"{label}:\n" + "\n".join([f"EVENT_ID: {eid}\n{txt}" for eid, txt in context])

        interest_block = format_context("User Interest Events", interest_context)
        search_block = format_context("Search History Events", search_context)
        full_context = f"{interest_block}\n\n{search_block}"

        # ✅ Step 6: Prompt LLM
        system_prompt = (
            "You are EventLens, a smart recommendation assistant. Based on the following user interests "
            "and recent search history, return the top 5 unique EVENT_IDs that are most relevant. "
            "Ensure that **at least one event is selected from the search history context** if any are present. "
            "Return only a numbered list of EVENT_IDs (no additional text)."
        )

        user_prompt = f"Context:\n{full_context}\n\nReturn 5 EVENT_IDs:"
        llm_output = generate_openai_response(system_prompt, user_prompt, model_name="gpt-4o", api_key=None)

        selected_ids = []
        for line in llm_output.splitlines():
            line = line.strip()
            if line and line[0].isdigit():
                parts = line.split()
                if parts:
                    try:
                        selected_ids.append(int(parts[-1]))
                    except:
                        continue

        if not selected_ids:
            return []

        placeholders = ', '.join(['%s'] * len(selected_ids))
        cursor.execute(f"""
            SELECT EVENT_ID, EVENT_TITLE, START_DATE, END_DATE, START_TIME, END_TIME,
                   OCCURRENCES, FULL_ADDRESS, LOCATION, ADMISSION, DESCRIPTION,
                   EVENT_URL, IMAGE_S3_URL, CATEGORIES
            FROM EVENTLENS_DB.EDW.FACT_EVENTS_DETAILS
            WHERE EVENT_ID IN ({placeholders})
        """, tuple(selected_ids))

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

        df = pd.DataFrame(rows, columns=columns)
        return df.to_dict(orient="records")

    finally:
        if cursor:
            cursor.close()
        if conn:
            close_connection(conn)
