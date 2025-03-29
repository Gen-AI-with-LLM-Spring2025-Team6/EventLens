"""
Script to process and load Boston Gov data to Snowflake EDW
"""
import os
import json
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from openai import OpenAI
import logging
from datetime import datetime
from data_load.parameters.parameter_config import (
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE,
    OPENAI_API_KEY
)

# Website specific settings - defined in the script
WEBSITE_NAME = "boston_gov"
EDW_SCHEMA = "EDW"
EDW_TABLE = "FACT_EVENTS_DETAILS"  # Single table for all websites

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
            schema=EDW_SCHEMA,  # Use EDW schema for this connection
            role=SNOWFLAKE_ROLE,
            client_session_keep_alive=True
        )
        
        return connection
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {str(e)}")
        raise

def structure_event_data(text):
    """
    Send combined event text to OpenAI to structure and clean it
    
    Args:
        text (str): Combined event text
        
    Returns:
        str: Structured and cleaned event text
    """
    try:
        # Skip if no API key
        if not OPENAI_API_KEY:
            return text

        # Call OpenAI API
        client = OpenAI(api_key=OPENAI_API_KEY)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert at organizing and formatting event information. Your task is to take raw event data and structure it in a clean, consistent format that highlights the most important details."
                },
                {
                    "role": "user",
                    "content": f"Please structure the following Boston government event information in a clean, well-organized format. Make sure to highlight important details like event title, date, time, location, contact information, and description. Remove any duplicate or irrelevant information:\n\n{text}"
                }
            ],
            temperature=0.3,
            max_tokens=1000
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        logger.warning(f"Error structuring event data: {str(e)}")
        return text

def get_embedding(text, cursor):
    """
    Get text embedding from Snowflake
    
    Args:
        text (str): Text to embed
        cursor: Snowflake cursor
        
    Returns:
        list: Vector embedding
    """
    try:
        import json
        
        # Execute embedding function
        query = """
        SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_1024('snowflake-arctic-embed-l-v2.0', PARSE_JSON(%s))
        """
        cursor.execute(query, (json.dumps({"text": text}),))
        
        # Get result
        result = cursor.fetchone()
        
        return result[0] if result else None
        
    except Exception as e:
        logger.error(f"Error getting embedding: {str(e)}")
        return None

def load_to_edw(**context):
    """
    Process Boston Gov events data and load to Snowflake EDW table
    """
    try:
        # Get the events data file from XCom
        ti = context['ti']
        events_file = ti.xcom_pull(task_ids='load_to_staging', key='events_staged')
        
        # Read events from file
        with open(events_file, 'r') as f:
            records = json.load(f)
            
        # Get Snowflake connection for embeddings
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Convert to DataFrame
        df = pd.DataFrame(records)
        
        # For testing: Process only the first 5 events
        # df = df.iloc[0:5]
        
        # Function to combine all columns into a single text representation
        def combine_row_data(row):
            # Include Boston Gov specific fields like Contact, Email, Event_Timings
            combined_text = " | ".join([f"{col}: {row[col]}" for col in df.columns])
            return combined_text
            
        # Apply function to all rows
        df["Combined_Text"] = df.apply(combine_row_data, axis=1)
        
        # Structure and clean the combined text using OpenAI
        logger.info("Structuring Boston Gov event data using OpenAI...")
        df["Structured_Text"] = df["Combined_Text"].apply(structure_event_data)
        
        # Generate embeddings for each row using Snowflake Arctic
        logger.info("Generating vector embeddings...")
        df["Vector_Embedding"] = df["Structured_Text"].apply(lambda text: get_embedding(text, cursor))
        
        # Add additional columns
        df["Source_Website"] = WEBSITE_NAME
        df["Date_Load_Time"] = pd.to_datetime("now")
        
        # Rename columns to match Snowflake schema (uppercase)
        df.columns = df.columns.str.upper()
        
        # Full table name
        full_table_name = f"{EDW_SCHEMA}.{EDW_TABLE}"
        
        # Prepare DataFrame for writing to Snowflake
        # Select only columns present in the EDW table schema
        edw_columns = [
            "EVENT_TITLE", "IMAGE_URL", "S3_URL", "START_TIME", "END_TIME", "LOCATION",
            "FULL_ADDRESS", "CATEGORIES", "ADMISSION", "DESCRIPTION", "EVENT_URL", 
            "COMBINED_TEXT", "STRUCTURED_TEXT", "VECTOR_EMBEDDING", "SOURCE_WEBSITE", "DATE_LOAD_TIME"
        ]
        
        # Filter DataFrame to include only EDW columns
        df_edw = df.reindex(columns=edw_columns, fill_value=None)
        
        # Write to Snowflake
        logger.info(f"Loading Boston Gov data to {full_table_name}...")
        result = write_pandas(conn, df_edw, EDW_TABLE)
        
        logger.info(f"Data insertion to {full_table_name} result: {result}")
        
        # Close connection
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error processing and loading to EDW: {str(e)}")
        raise