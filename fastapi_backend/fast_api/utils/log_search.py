from fastapi_backend.fast_api.config.db_connection import snowflake_connection, close_connection
from datetime import datetime

def log_user_search(user_id: int, query: str):
    conn = snowflake_connection()
    cursor = conn.cursor()
    try:
        insert_query = """
        INSERT INTO EVENTLENS_DB.EDW.USER_SEARCH_HISTORY (USER_ID, SEARCH_QUERY, TIMESTAMP)
        VALUES (%s, %s, %s)
        """
        cursor.execute(insert_query, (user_id, query, datetime.utcnow()))
        conn.commit()
    finally:
        cursor.close()
        close_connection(conn)
