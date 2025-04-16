from fastapi_backend.fast_api.config.db_connection import snowflake_connection, close_connection

def get_sample_events():
    conn = None
    cursor = None
    try:
        conn = snowflake_connection()
        cursor = conn.cursor()

        query = """
        SELECT 
            EVENT_ID, EVENT_TITLE, START_DATE, END_DATE, START_TIME, END_TIME,
            OCCURRENCES, FULL_ADDRESS, LOCATION, ADMISSION, DESCRIPTION,
            EVENT_URL, IMAGE_S3_URL, CATEGORIES
        FROM EVENTLENS_DB.EDW.FACT_EVENTS_DETAILS
        LIMIT 12
        """

        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

        return [dict(zip(columns, row)) for row in rows]

    finally:
        if cursor:
            cursor.close()
        if conn:
            close_connection(conn)
