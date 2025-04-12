import os
import json
import pandas as pd
import logging
from datetime import datetime
from snowflake.connector.pandas_tools import write_pandas
from data_load.connectors.db_connection import get_snowflake_connection
from data_load.helpers.utils import (
    preprocess_text_column,
    handle_missing_end_date,
    classify_event_using_cortex,
    parallelize_structuring_and_embedding,
    is_event_unique
)

# Website-specific configs
WEBSITE_NAME = "boston_gov"
EDW_SCHEMA = "EDW"
EDW_TABLE = "FACT_EVENTS_DETAILS"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

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

        df['END_TIME'] = df.apply(
            lambda row: handle_missing_end_date(row['DESCRIPTION'], cursor)
            if not row['END_TIME'] or str(row['END_TIME']).strip().lower() in ["", "not available", "none"]
            else row['END_TIME'],
            axis=1
        )

        df['CATEGORIES'] = df.apply(
            lambda row: classify_event_using_cortex(
                row['EVENT_TITLE'], row['DESCRIPTION'], row['FULL_ADDRESS'], row['START_TIME'], cursor
            ) if not row['CATEGORIES'] or str(row['CATEGORIES']).strip().lower() in ["no categories", "none", "","no end time"]
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
