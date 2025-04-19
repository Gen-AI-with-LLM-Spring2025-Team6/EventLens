"""
Script to load events data to Snowflake staging table
"""
import os
import json
import pandas as pd
import snowflake.connector
import logging
from data_load.parameters.parameter_config import (
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE, 
    SNOWFLAKE_ROLE
)

# Website specific settings
WEBSITE_NAME = "boston_calendar"
STAGING_TABLE = f"{WEBSITE_NAME.upper()}_EVENTS_DETAILS"

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def get_snowflake_connection():
    try:
        return snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role=SNOWFLAKE_ROLE,
            client_session_keep_alive=True
        )
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {str(e)}")
        raise

def load_to_staging(**context):
    try:
        ti = context['ti']
        events_file = ti.xcom_pull(task_ids='process_images', key='events_with_images')

        with open(events_file, 'r') as f:
            events = json.load(f)
        df_events = pd.DataFrame(events)

        conn = get_snowflake_connection()
        cursor = conn.cursor()
        full_table_name = f"{SNOWFLAKE_SCHEMA}.{STAGING_TABLE}"

        # Truncate the table before inserting
        cursor.execute(f"TRUNCATE TABLE {full_table_name}")
        logger.info(f"Truncated table {full_table_name}")

        insert_query = f"""
        INSERT INTO {full_table_name} 
        (EVENT_TITLE, IMAGE_URL, S3_URL, START_DATE, END_DATE, START_TIME, END_TIME,
         OCCURRENCES, LOCATION, FULL_ADDRESS, CATEGORIES, ADMISSION, DESCRIPTION, EVENT_URL)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        data_to_insert = [
            (
                row.get('Event_Title', ''),
                row.get('Image_URL', ''),
                row.get('S3_URL', ''),
                row.get('Start_Date', ''),
                row.get('End_Date', ''),
                row.get('Start_Time', ''),
                row.get('End_Time', ''),
                row.get('Occurrences', ''),
                row.get('Location', ''),
                row.get('Full_Address', ''),
                row.get('Categories', ''),
                row.get('Admission', ''),
                row.get('Description', ''),
                row.get('Event_URL', '')
            )
            for row in events
        ]

        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        logger.info(f"Inserted {len(data_to_insert)} records into {full_table_name}")

        cursor.close()
        conn.close()

        context['ti'].xcom_push(key='events_staged', value=events_file)

    except Exception as e:
        logger.error(f"Error loading to Snowflake staging: {str(e)}")
        raise
