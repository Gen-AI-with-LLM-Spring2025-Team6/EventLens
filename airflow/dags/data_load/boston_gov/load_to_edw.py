import os
import re
import json
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from concurrent.futures import ThreadPoolExecutor
import logging
from datetime import datetime
from data_load.parameters.parameter_config import (
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE
)

# Website settings
WEBSITE_NAME = "boston_gov"
EDW_SCHEMA = "EDW"
EDW_TABLE = "FACT_EVENTS_DETAILS"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# -----------------------------------------------------------------------------
# Utility Functions
# -----------------------------------------------------------------------------

def get_snowflake_connection():
    try:
        return snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=EDW_SCHEMA,
            role=SNOWFLAKE_ROLE,
            client_session_keep_alive=True
        )
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {str(e)}")
        raise

def structure_event_data(text, cursor):
    try:
        prompt = f"""
        You are an expert at organizing and formatting event information. Your task is to take raw event data and structure it in a clean, consistent format that highlights the most important details.

        Please structure the following event information in a clean, well-organized format. Make sure to highlight important details like event title, date, time, location, and description. Remove any duplicate or irrelevant information. The output should contain only the result with no other explanations or wordings.

        Event information:
        {text}
        """
        query = "SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', %s)"
        cursor.execute(query, (prompt,))
        result = cursor.fetchone()
        return result[0] if result and result[0] else text
    except Exception as e:
        logger.warning(f"Error structuring event data: {str(e)}")
        return text

def get_embedding(text, cursor):
    try:
        query = """
        SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_1024('snowflake-arctic-embed-l-v2.0', PARSE_JSON(%s))
        """
        cursor.execute(query, (json.dumps({"text": text}),))
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Error getting embedding: {str(e)}")
        return None

def classify_event_using_cortex(title, description, address, dates, cursor):
    try:
        prompt = f"""
        You will be provided with event details, and your task is to classify the event into one or more categories from the following list:
        Concert, Sports, Festival, Exhibition, Theater, Comedy Show, Food & Drink, Networking, Educational, Family-Friendly, Tech Conference, Other.

        Event Details:
        - Title: {title}
        - Description: {description}
        - Address: {address}
        - Event Dates: {dates}

        Please classify this event into one or more categories from the list above and return the category names as a list.
        """
        query = "SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', %s)"
        cursor.execute(query, (prompt,))
        result = cursor.fetchone()
        return json.dumps(result[0]) if result else "Other"
    except Exception as e:
        logger.error(f"Error classifying event: {str(e)}")
        return "Other"

def handle_missing_end_date(description, cursor):
    try:
        prompt = f"""
        You will be provided with event details, and your task is to extract the End Time from the description in the following format: "YYYY-MM-DD HH:MM:SS".

        Event Description: {description}

        Please return only the End Time in the specified format. If not available, return - Not Available.
        """
        return structure_event_data(prompt, cursor)
    except Exception as e:
        logger.error(f"Error extracting end date: {str(e)}")
        return "Not Available"

def preprocess_text_column(text):
    if text:
        text = text.strip()
        text = re.sub(r'(\w)([A-Z])', r'\1 \2', text)
        return text.capitalize()
    return text

def parallelize_structuring_and_embedding(df, cursor):
    def process_row(item):
        index, row = item
        combined_text = " | ".join([f"{col}: {row[col]}" for col in df.columns if col not in ['S3_URL', 'EVENT_URL', 'IMAGE_URL']])
        structured = structure_event_data(combined_text, cursor)
        embedding = get_embedding(structured, cursor)
        return structured, embedding

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(process_row, df.iterrows()))
    structured_texts, embeddings = zip(*results)
    df['STRUCTURED_TEXT'] = structured_texts
    df['VECTOR_EMBEDDING'] = embeddings
    return df

def is_event_unique(embedding, cursor, threshold=0.9):
    try:
        query = """
        SELECT VECTOR_COSINE_SIMILARITY(
          VECTOR_EMBEDDING::VECTOR(FLOAT, 1024),
          PARSE_JSON(%s)::VECTOR(FLOAT, 1024)
        ) AS similarity
        FROM EDW.FACT_EVENTS_DETAILS
        ORDER BY similarity DESC
        LIMIT 1
        """
        cursor.execute(query, (json.dumps(embedding),))
        result = cursor.fetchone()
        max_similarity = float(result[0]) if result and result[0] is not None else 0
        return max_similarity < threshold
    except Exception as e:
        logger.error(f"Error checking vector similarity: {str(e)}")
        return True

# -----------------------------------------------------------------------------
# Main ETL Function
# -----------------------------------------------------------------------------

def load_to_edw(**context):
    try:
        ti = context['ti']
        events_file = ti.xcom_pull(task_ids='load_to_staging', key='events_staged')

        with open(events_file, 'r') as f:
            records = json.load(f)
        df = pd.DataFrame(records)
        df.columns = list(map(lambda col: col.upper(), df.columns))

        conn = get_snowflake_connection()
        cursor = conn.cursor()

        df['EVENT_TITLE'] = df['EVENT_TITLE'].apply(preprocess_text_column)
        df['DESCRIPTION'] = df['DESCRIPTION'].apply(preprocess_text_column)
        df['FULL_ADDRESS'] = df['FULL_ADDRESS'].apply(preprocess_text_column)

        # END_TIME: Only update if missing or "Not Available"
        df['END_TIME'] = df.apply(
            lambda row: handle_missing_end_date(row['DESCRIPTION'], cursor)
            if not row['END_TIME'] or str(row['END_TIME']).strip().lower() in ["", "not available", "none"]
            else row['END_TIME'],
            axis=1
        )

        # CATEGORIES: Only update if missing or "No Categories"
        df['CATEGORIES'] = df.apply(
            lambda row: classify_event_using_cortex(
                row['EVENT_TITLE'], row['DESCRIPTION'], row['FULL_ADDRESS'], row['START_TIME'], cursor
            ) if not row['CATEGORIES'] or str(row['CATEGORIES']).strip().lower() in ["no categories", "none", ""]
            else row['CATEGORIES'],
            axis=1
        )

        df = parallelize_structuring_and_embedding(df, cursor)

        logger.info("Filtering near-duplicate events using vector similarity...")
        unique_rows = []
        for _, row in df.iterrows():
            embedding = row['VECTOR_EMBEDDING']
            if is_event_unique(embedding, cursor, threshold=0.9):
                unique_rows.append(row)
            else:
                logger.info(f"Skipping similar event: {row['EVENT_TITLE']}")

        df = pd.DataFrame(unique_rows)

        if df.empty:
            logger.info("No new unique events to insert.")
            return

        df["SOURCE_WEBSITE"] = WEBSITE_NAME
        df["DATE_LOAD_TIME"] = pd.to_datetime("now")
        df.columns = df.columns.str.upper()

        edw_columns = [
            "EVENT_TITLE", "IMAGE_URL", "S3_URL", "START_TIME", "END_TIME", "LOCATION",
            "FULL_ADDRESS", "CATEGORIES", "ADMISSION", "DESCRIPTION", "EVENT_URL",
            "STRUCTURED_TEXT", "VECTOR_EMBEDDING", "SOURCE_WEBSITE", "DATE_LOAD_TIME"
        ]
        df_edw = df.reindex(columns=edw_columns, fill_value=None)

        logger.info(f"Inserting {len(df_edw)} unique events...")
        write_pandas(conn, df_edw, EDW_TABLE)

        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(f"Error loading to EDW: {str(e)}")
        raise
