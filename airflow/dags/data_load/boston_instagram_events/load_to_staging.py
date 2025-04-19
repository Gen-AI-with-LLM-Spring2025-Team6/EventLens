"""
Script to load Instagram events data to Snowflake staging table
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

# Website specific settings - defined in the script
WEBSITE_NAME = "instagram_events"
STAGING_TABLE = "BOSTON_INSTAGRAM_EVENTS_DETAILS"

# Configure logging
logger = logging.getLogger(__name__)

def get_snowflake_connection():
    """
    Create and return a Snowflake connection
    
    Returns:
        connection: Snowflake connection object
    """
    try:
        # Connect to Snowflake
        connection = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role=SNOWFLAKE_ROLE,
            client_session_keep_alive=True
        )
        
        return connection
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {str(e)}")
        raise

def load_to_staging(**context):
    """
    Load Instagram events data to Snowflake staging table
    """
    try:
        # Get the events data file from XCom
        ti = context['ti']
        events_file = ti.xcom_pull(task_ids='process_media', key='events_with_media')
        
        # Read events from file
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
        
        # Prepare insert query for Instagram specific fields
        insert_query = f"""
        INSERT INTO {full_table_name} 
        (EVENT_TITLE, IMAGE_URL, VIDEO_URL, S3_IMAGE_URL, S3_VIDEO_URL, S3_THUMBNAIL_URL,
         START_TIME, END_TIME, LOCATION, FULL_ADDRESS, CATEGORIES, ADMISSION, 
         DESCRIPTION, EVENT_URL, SOURCE_ACCOUNT, POST_TYPE, POST_ID)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Prepare data for insertion
        data_to_insert = [
            (row.get('Event_Title', ''), 
             row.get('Image_URL', ''),
             row.get('Video_URL', ''),
             row.get('S3_Image_URL', ''),
             row.get('S3_Video_URL', ''),
             row.get('S3_Thumbnail_URL', ''),
             row.get('Start_Time', ''), 
             row.get('End_Time', ''), 
             row.get('Location', ''), 
             row.get('Full_Address', ''), 
             row.get('Categories', ''), 
             row.get('Admission', ''), 
             row.get('Description', ''), 
             row.get('Event_URL', ''),
             row.get('Source_Account', ''),
             row.get('Post_Type', ''),
             row.get('Post_ID', ''))
            for row in events
        ]
        
        # Execute insert
        cursor.executemany(insert_query, data_to_insert)
        
        # Commit and close
        conn.commit()
        logger.info(f"Inserted {len(data_to_insert)} records into {full_table_name}")
        
        cursor.close()
        conn.close()
        
        # Pass the file path to the next task
        context['ti'].xcom_push(key='events_staged', value=events_file)
        
    except Exception as e:
        logger.error(f"Error loading to Snowflake staging: {str(e)}")
        raise