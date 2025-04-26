def vector_search_snowflake(query, conn, table_name, embedding_col, text_col, model_name="snowflake-arctic-embed-l-v2.0", limit=5):
    """
    Perform vector similarity search in Snowflake using the Cortex embedding function.

    Args:
        query (str): The search query.
        conn: Snowflake connection object.
        table_name (str): Fully qualified table name (e.g., DATABASE.SCHEMA.TABLE).
        embedding_col (str): Column name containing vector embeddings.
        text_col (str): Column name with searchable event text.
        model_name (str): Embedding model used for the query.
        limit (int): Maximum number of results to return.

    Returns:
        list: List of (event_id, text, similarity_score) tuples.
    """
    try:
        cursor = conn.cursor()
        sql = f"""
        SELECT
            EVENT_ID,
            {text_col},
            VECTOR_COSINE_SIMILARITY(
                {embedding_col}::VECTOR(FLOAT, 1024),
                SNOWFLAKE.CORTEX.EMBED_TEXT_1024(%s, %s)
            ) AS similarity
        FROM {table_name}
        WHERE source_website != 'boston_central'
        ORDER BY similarity DESC
        LIMIT {limit}
        """
        cursor.execute(sql, (model_name, query))
        results = cursor.fetchall()
        cursor.close()
        return results

    except Exception as e:
        print(f"Snowflake vector search failed: {e}")
        return []
