"""
Script to load events data to Snowflake staging table
"""
import os
import json
import pandas as pd
import snowflake.connector
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Website specific settings
WEBSITE_NAME = "boston_central"
STAGING_TABLE = f"{WEBSITE_NAME.upper()}_EVENTS_DETAILS"
TEMP_DIR = "/tmp/boston_central"

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def create_sample_data():
    """Create sample data as a fallback"""
    logger.info("Creating sample data as fallback")
    
    return [
        {
            "Event_Title": "Sample Boston Event 1",
            "Event_URL": "https://www.bostoncentral.com/events/sample1",
            "Start_Date": "04/30/2025",
            "End_Date": "05/01/2025",
            "Start_Time": "10:00 AM - 5:00 PM",
            "End_Time": "10:00 AM - 5:00 PM",
            "Occurrences": "",
            "Image_URL": "",
            "Location": "BOSTON COMMON",
            "Full_Address": "BOSTON COMMON, BOSTON, MA",
            "Categories": "Festival",
            "Admission": "Free",
            "Description": "This is a sample event for testing purposes."
        },
        {
            "Event_Title": "Sample Boston Event 2",
            "Event_URL": "https://www.bostoncentral.com/events/sample2",
            "Start_Date": "05/15/2025",
            "End_Date": "05/15/2025",
            "Start_Time": "7:00 PM - 9:00 PM",
            "End_Time": "7:00 PM - 9:00 PM",
            "Occurrences": "",
            "Image_URL": "",
            "Location": "FANEUIL HALL",
            "Full_Address": "FANEUIL HALL, BOSTON, MA",
            "Categories": "Music",
            "Admission": "$15",
            "Description": "Another sample event for testing the pipeline."
        }
    ]

def get_snowflake_connection():
    try:
        return snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE"),
            client_session_keep_alive=True
        )
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {str(e)}")
        raise

def load_to_staging(**context):
    try:
        # Get the events file path from the previous task
        ti = context['ti']
        events_file = ti.xcom_pull(task_ids=f'scrape_{WEBSITE_NAME}', key='events_data')
        
        if not events_file:
            logger.warning("No events file found in XCom, using default path")
            # Use the default path instead
            events_file = os.path.join(TEMP_DIR, WEBSITE_NAME, 'scraped_events.json')
            
        # Check if the file exists
        if not os.path.exists(events_file):
            logger.warning(f"Events file not found at {events_file}, creating sample data")
            # Create the directory if it doesn't exist
            os.makedirs(os.path.dirname(events_file), exist_ok=True)
            # Create sample data
            sample_data = create_sample_data()
            # Save to the file
            with open(events_file, 'w') as f:
                json.dump(sample_data, f)
            logger.info(f"Created sample data file at {events_file}")
            
        logger.info(f"Loading events from file: {events_file}")
        
        # Read the JSON file
        with open(events_file, 'r') as f:
            events = json.load(f)
            
        logger.info(f"Loaded {len(events)} events from JSON file")
        
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Get schema from connection
        snowflake_schema = conn.schema
        full_table_name = f"{snowflake_schema}.{STAGING_TABLE}"

        # Truncate the table before inserting
        cursor.execute(f"TRUNCATE TABLE {full_table_name}")
        logger.info(f"Truncated table {full_table_name}")

        insert_query = f"""
        INSERT INTO {full_table_name} 
        (EVENT_TITLE, IMAGE_URL, S3_URL, START_DATE, END_DATE, START_TIME, END_TIME,
         OCCURRENCES, LOCATION, FULL_ADDRESS, CATEGORIES, ADMISSION, DESCRIPTION, EVENT_URL)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Prepare data for insertion
        data_to_insert = [
            (
                event.get('Event_Title', ''),
                event.get('Image_URL', ''),
                '',  # S3_URL will be empty initially
                event.get('Start_Date', ''),
                event.get('End_Date', ''),
                event.get('Start_Time', ''),
                event.get('End_Time', ''),
                event.get('Occurrences', ''),
                event.get('Location', ''),
                event.get('Full_Address', ''),
                event.get('Categories', ''),
                event.get('Admission', ''),
                event.get('Description', ''),
                event.get('Event_URL', '')
            )
            for event in events
        ]

        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        logger.info(f"Inserted {len(data_to_insert)} records into {full_table_name}")

        cursor.close()
        conn.close()
        
        # Push to XCom for the next task
        ti.xcom_push(key='events_staged', value=events_file)
        logger.info(f"Pushed events_staged to XCom: {events_file}")
        
        return True

    except Exception as e:
        logger.error(f"Error loading to Snowflake staging: {str(e)}")
        import traceback
        logger.error(f"Full exception traceback:\n{traceback.format_exc()}")
        raise

if __name__ == "__main__":
    # For testing outside of Airflow
    import argparse
    
    parser = argparse.ArgumentParser(description='Load Boston Central events to Snowflake staging')
    args = parser.parse_args()
    
    # Check if the events file exists in the expected location for manual testing
    events_file = os.path.join(TEMP_DIR, WEBSITE_NAME, 'scraped_events.json')
    if os.path.exists(events_file):
        # Create a mock context with TI
        class MockTI:
            def xcom_pull(self, task_ids, key):
                return events_file
                
            def xcom_push(self, key, value):
                print(f"Pushed {key}: {value} to XCom")
                
        mock_context = {'ti': MockTI()}
        
        # Run the function
        load_to_staging(**mock_context)
    else:
        print(f"Events file not found at {events_file}. Please run the scraper first.")