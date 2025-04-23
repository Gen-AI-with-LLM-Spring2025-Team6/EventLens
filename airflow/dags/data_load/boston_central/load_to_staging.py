import os
import argparse
import pandas as pd
import snowflake.connector
import logging
from dotenv import load_dotenv

load_dotenv()  # Loads from .env file by default

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

# Website specific settings
WEBSITE_NAME = "boston_central"
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

def load_to_staging(csv_file_path):
    try:
        logger.info(f"Reading CSV file from: {csv_file_path}")
        
        # Read the CSV file
        df_events = pd.read_csv(csv_file_path)
        
        # Check if file was loaded properly
        logger.info(f"Loaded {len(df_events)} records from CSV file")
        
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

        # Define a helper function to get values with NA for null fields
        def get_value(row, column):
            if column in row and pd.notna(row[column]):
                return str(row[column])
            return 'NA'

        # Prepare data for insertion
        data_to_insert = [
            (
                get_value(row, 'Event_Title'),
                get_value(row, 'Image_URL'),
                get_value(row, 'S3_URL'),
                get_value(row, 'Start_Date'),
                get_value(row, 'End_Date'),
                get_value(row, 'Start_Time'),
                get_value(row, 'End_Time'),
                get_value(row, 'Occurrences'),
                get_value(row, 'Location'),
                get_value(row, 'Full_Address'),
                get_value(row, 'Categories'),
                get_value(row, 'Admission'),
                get_value(row, 'Description'),
                get_value(row, 'Event_URL')
            )
            for _, row in df_events.iterrows()
        ]

        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        logger.info(f"Inserted {len(data_to_insert)} records into {full_table_name}")

        cursor.close()
        conn.close()
        
        print(f"Successfully uploaded {len(data_to_insert)} records to Snowflake")
        return True

    except Exception as e:
        logger.error(f"Error loading to Snowflake staging: {str(e)}")
        raise

# if __name__ == "__main__":
#     # Set up argument parsing
#     parser = argparse.ArgumentParser(description='Load CSV data to Snowflake staging table')
#     parser.add_argument('--csv_file', required=True, help='Path to the CSV file containing event data')
    
#     args = parser.parse_args()
    
#     # Execute the loading function
#     load_to_staging(args.csv_file)
load_to_staging("/Users/agash/Personal Docs/MS IS/Spring 2025/GenAI w LLM for DE/GitHub/EventLens/airflow/dags/data_load/boston_central/files/boston_events_temp.csv")