import os
import streamlit as st
import snowflake.connector

def init_snowflake_connection(user=None, password=None, account=None, warehouse=None, database=None, schema=None):
    """
    Initialize connection to Snowflake using credentials from .env file or provided parameters.
    
    Args:
        user (str, optional): Snowflake username
        password (str, optional): Snowflake password
        account (str, optional): Snowflake account
        warehouse (str, optional): Snowflake warehouse
        database (str, optional): Snowflake database
        schema (str, optional): Snowflake schema
    
    Returns:
        connection: Snowflake connection object or None if connection fails
    """
    try:
        conn = snowflake.connector.connect(
            user=user or os.getenv("SNOWFLAKE_USER"),
            password=password or os.getenv("SNOWFLAKE_PASSWORD"),
            account=account or os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=warehouse or os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=database or os.getenv("SNOWFLAKE_DATABASE"),
            schema=schema or os.getenv("SNOWFLAKE_SCHEMA")
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
        return None

def vector_search_snowflake(query, conn, table_name, embedding_col, text_col, model_name="snowflake-arctic-embed-l-v2.0", limit=5):
    """
    Perform vector similarity search in Snowflake using the Cortex embedding function.
    
    Args:
        query (str): The search query
        conn: Snowflake connection
        table_name (str): Name of the table containing event data
        embedding_col (str): Name of the column containing embeddings
        text_col (str): Name of the column containing event text
        model_name (str): Name of the embedding model to use
        limit (int): Maximum number of results to return
    
    Returns:
        list: List of tuples containing (event_id, text, similarity_score)
    """
    try:
        cursor = conn.cursor()
        sql = f"""
        SELECT
          EVENT_ID,
          {text_col},
          VECTOR_COSINE_SIMILARITY(
            {embedding_col}::VECTOR(FLOAT, 1024),
            SNOWFLAKE.CORTEX.EMBED_TEXT_1024('{model_name}', %s)
          ) AS similarity
        FROM {table_name}
        ORDER BY similarity DESC
        LIMIT {limit}
        """
        cursor.execute(sql, (query,))
        results = cursor.fetchall()
        cursor.close()
        return results
    except Exception as e:
        st.error(f"Error executing Snowflake query: {e}")
        return []