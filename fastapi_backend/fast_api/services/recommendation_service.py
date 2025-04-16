#from fastapi_backend.fast_api.config.db_connection import snowflake_connection, close_connection
#from typing import List, Dict, Any
#import pandas as pd
#
#def fetch_recommended_events(user_id: int) -> List[Dict[str, Any]]:
#    conn = None
#    cursor = None
#
#    try:
#        conn = snowflake_connection()
#        cursor = conn.cursor()
#
#        query = """
#        SELECT 
#            f.EVENT_TITLE, f.START_DATE, f.END_DATE, f.START_TIME, f.END_TIME,
#            f.OCCURRENCES, f.FULL_ADDRESS, f.LOCATION, f.ADMISSION, f.DESCRIPTION,f.EVENT_URL, f.IMAGE_S3_URL
#        FROM EVENTLENS_DB.EDW.RECOMMENDATION r
#        JOIN EVENTLENS_DB.EDW.FACT_EVENTS_DETAILS f
#            ON r.EVENT_ID = f.EVENT_ID
#        WHERE r.USER_ID = %s
#        ORDER BY r.SCORE DESC NULLS LAST, f.START_DATE
#        """
#
#        cursor.execute(query, (user_id,))
#        columns = [col[0] for col in cursor.description]
#        rows = cursor.fetchall()
#
#        if not rows:
#            return []
#
#        df = pd.DataFrame(rows, columns=columns)
#        return df.to_dict(orient="records")
#
#    finally:
#        if cursor:
#            cursor.close()
#        if conn:
#            close_connection(conn)

from fastapi_backend.fast_api.config.db_connection import snowflake_connection, close_connection
from typing import List, Dict, Any
import json
import pandas as pd

def fetch_recommended_events(user_id: int) -> List[Dict[str, Any]]:
    conn = None
    cursor = None
    user_interests = ""

    try:
        conn = snowflake_connection()
        cursor = conn.cursor()

        # Step 1: Fetch user interests
        cursor.execute(
            "SELECT INTERESTS FROM EVENTLENS_DB.EDW.USERDETAILS WHERE USER_ID = %s",
            (user_id,)
        )
        interest_row = cursor.fetchone()
        if interest_row:
            user_interests = interest_row[0]

        # Step 2: Run Cortex Search to get event IDs
        search_query = f'''
        SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            'EVENTLENS_DB.EDW.EVENTS_SEARCH',
            '{{
                "query": "{user_interests}",
                "limit": 5,
                "columns": ["EVENT_ID"]
            }}'
        ) AS result
        '''

        cursor.execute(search_query)
        result_rows = cursor.fetchall()
        event_ids = []

        for row in result_rows:
            parsed = json.loads(row[0])
            if isinstance(parsed, dict) and "results" in parsed:
                for entry in parsed["results"]:
                    if "EVENT_ID" in entry:
                        event_ids.append(entry["EVENT_ID"])

        if not event_ids:
            return []

        # Step 3: Fetch full event details for the recommended event IDs
        placeholders = ', '.join(['%s'] * len(event_ids))
        event_query = f'''
        SELECT 
            EVENT_TITLE, START_DATE, END_DATE, START_TIME, END_TIME,
            OCCURRENCES, FULL_ADDRESS, LOCATION, ADMISSION, DESCRIPTION,
            EVENT_URL, IMAGE_S3_URL
        FROM EVENTLENS_DB.EDW.FACT_EVENTS_DETAILS
        WHERE EVENT_ID IN ({placeholders})
        '''

        cursor.execute(event_query, tuple(event_ids))
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

        df = pd.DataFrame(rows, columns=columns)
        return df.to_dict(orient="records")

    finally:
        if cursor:
            cursor.close()
        if conn:
            close_connection(conn)