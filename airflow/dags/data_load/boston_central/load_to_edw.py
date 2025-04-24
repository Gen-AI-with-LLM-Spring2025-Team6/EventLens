import pandas as pd
import logging
import re
from snowflake.connector.pandas_tools import write_pandas
from data_load.connectors.db_connection import get_snowflake_connection
from data_load.helpers.utils import (
    preprocess_text_column,
    classify_event_into_group,
    parallelize_structuring_and_embedding,
    is_event_unique,
    format_time_from_fields,  # Add the new functions
    extract_start_time,       # Add the new functions
    extract_end_time          # Add the new functions
)

# Website-specific configs
WEBSITE_NAME = "boston_central"
STAGING_TABLE = "STAGING.BOSTON_CENTRAL_EVENTS_DETAILS"
EDW_TABLE = "EDW.FACT_EVENTS_DETAILS"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def load_to_edw(**context):
    try:
        logger.info("Starting load_to_edw process")
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        # Read records from staging table
        select_query = f"""
            SELECT EVENT_TITLE, S3_URL, START_DATE, END_DATE, START_TIME, END_TIME,
                   OCCURRENCES, LOCATION, FULL_ADDRESS, CATEGORIES, ADMISSION, DESCRIPTION,
                   EVENT_URL
            FROM {STAGING_TABLE}
        """
        cursor.execute(select_query)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)

        if df.empty:
            logger.info("No records found in staging.")
            return
        else: 
            logger.info(f"Found {len(df)} records in staging.")

        # Rename S3_URL to IMAGE_S3_URL
        df.rename(columns={"S3_URL": "IMAGE_S3_URL"}, inplace=True)

        # Preprocessing
        df['EVENT_TITLE'] = df['EVENT_TITLE'].apply(preprocess_text_column)
        df['DESCRIPTION'] = df['DESCRIPTION'].apply(preprocess_text_column)
        df['FULL_ADDRESS'] = df['FULL_ADDRESS'].apply(preprocess_text_column)

        # Format start and end datetime strings using Cortex
        logger.info("Formatting start and end datetimes...")
        df['START_TIME'] = df.apply(
            lambda row: extract_start_time(
                row['START_DATE'], row['START_TIME'], cursor
            ),
            axis=1
        )
        
        df['END_TIME'] = df.apply(
            lambda row: extract_end_time(
                row['END_DATE'], row['END_TIME'], cursor
            ),
            axis=1
        )

        # Classify events into categories using your helper
        df['CATEGORIES'] = df.apply(
            lambda row: classify_event_into_group(
                row['EVENT_TITLE'], row['DESCRIPTION'], row['FULL_ADDRESS'], row['CATEGORIES'], cursor
            ),
            axis=1
        )

        # Structure and embed text
        df = parallelize_structuring_and_embedding(df, cursor)

        # Filter duplicates using embeddings
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
        df.columns = df.columns.str.upper()

        # Reorder columns as per EDW schema, including the new formatted datetime fields
        edw_columns = [
            "EVENT_TITLE", "IMAGE_S3_URL", "START_DATE", "END_DATE", "START_TIME", "END_TIME",
            "OCCURRENCES", "LOCATION", "FULL_ADDRESS", "CATEGORIES", "ADMISSION", "DESCRIPTION",
            "EVENT_URL", "STRUCTURED_TEXT", "VECTOR_EMBEDDING", "SOURCE_WEBSITE"
        ]
        df_edw = df.reindex(columns=edw_columns, fill_value=None)

        logger.info(f"Inserting {len(df_edw)} unique records into {EDW_TABLE}")
        write_pandas(conn, df_edw, table_name=EDW_TABLE.split('.')[-1], schema=EDW_TABLE.split('.')[0])

        cursor.close()
        conn.close()
        
        # Push results to XCom
        if 'ti' in context:
            context['ti'].xcom_push(key='edw_loaded', value=True)
            context['ti'].xcom_push(key='edw_record_count', value=len(df_edw))
            logger.info(f"Pushed results to XCom: {len(df_edw)} records loaded")

        logger.info(f"Successfully loaded {len(df_edw)} records to EDW")
        return True

    except Exception as e:
        logger.error(f"Error loading to EDW: {str(e)}")
        import traceback
        logger.error(f"Full exception traceback:\n{traceback.format_exc()}")
        
        # Still push a value to XCom to indicate failure
        if 'ti' in context:
            context['ti'].xcom_push(key='edw_loaded', value=False)
            context['ti'].xcom_push(key='edw_error', value=str(e))
        
        raise

if __name__ == "__main__":
    # For testing outside of Airflow
    # Create a mock context for testing
    class MockTI:
        def xcom_push(self, key, value):
            print(f"Pushed {key}: {value} to XCom")
    
    mock_context = {'ti': MockTI()}
    
    # Run the function
    load_to_edw(**mock_context)