"""
Script to load Boston Gov events data to Snowflake staging table
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
WEBSITE_NAME = "boston_gov"
STAGING_TABLE = f"{WEBSITE_NAME.upper()}_EVENTS_DETAILS"

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def get_snowflake_connection():
    """
    Create and return a Snowflake connection
    """
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
    """
    Load Boston Gov events data to Snowflake staging table
    """
    try:
        # Get the events data file from XCom
        ti = context['ti']
        events_file = ti.xcom_pull(task_ids='process_images', key='events_with_images')

        # Read JSON
        with open(events_file, 'r') as f:
            events = json.load(f)
        
        # Convert to DataFrame
        df_events = pd.DataFrame(events)

        # Connect to Snowflake
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        # Full table name
        full_table_name = f"{SNOWFLAKE_SCHEMA}.{STAGING_TABLE}"

        # Truncate the table before inserting
        cursor.execute(f"TRUNCATE TABLE {full_table_name}")
        logger.info(f"Truncated table {full_table_name}")

        # Insert query with new structure
        insert_query = f"""
        INSERT INTO {full_table_name}
        (
            Event_Title, Image_URL, S3_URL, Start_Date, End_Date,
            Start_Time, End_Time, Occurrences, Location, Full_Address,
            Categories, Admission, Description, Event_URL, Contact, Email
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Prepare data
        data_to_insert = [
            (
                row.get("Event_Title", ""),
                row.get("Image_URL", ""),
                row.get("S3_URL", ""),
                row.get("Start_Date", ""),
                row.get("End_Date", ""),
                row.get("Start_Time", ""),
                row.get("End_Time", ""),
                row.get("Occurrences", ""),
                row.get("Location", ""),
                row.get("Full_Address", ""),
                row.get("Categories", ""),
                row.get("Admission", ""),
                row.get("Description", ""),
                row.get("Event_URL", ""),
                row.get("Contact", ""),
                row.get("Email", "")
            )
            for row in events
        ]

        # Execute insert
        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        logger.info(f"Inserted {len(data_to_insert)} records into {full_table_name}")

        # Cleanup
        cursor.close()
        conn.close()

        # Pass path to next task
        context['ti'].xcom_push(key='events_staged', value=events_file)

    except Exception as e:
        logger.error(f"Error loading to Snowflake staging: {str(e)}")
        raise
