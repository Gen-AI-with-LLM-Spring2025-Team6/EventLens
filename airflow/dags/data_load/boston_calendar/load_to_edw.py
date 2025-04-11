import os
import json
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from data_load.parameters.parameter_config import (
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE
)

# Website specific settings
WEBSITE_NAME = "boston_calendar"
EDW_SCHEMA = "EDW"
EDW_TABLE = "FACT_EVENT_DETAILS_1"  # Single table for all websites

# Configure logging
logger = logging.getLogger(__name__)

def get_snowflake_connection():
    """Create and return a Snowflake connection."""
    try:
        connection = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=EDW_SCHEMA,
            role=SNOWFLAKE_ROLE,
            client_session_keep_alive=True
        )
        return connection
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {str(e)}")
        raise

def structure_event_data(text, cursor):
    """Send combined event text to Snowflake Cortex to structure and clean it."""
    try:
        event_prompt = f"""
        You are an expert at organizing and formatting event information. Your task is to take raw event data and structure it in a clean, consistent format that highlights the most important details.

        Please structure the following event information in a clean, well-organized format. Make sure to highlight important details like event title, date, time, location, and description. Remove any duplicate or irrelevant information. The output should contain only the result with no other explanations or wordings.

        Event information:
        {text}
        """
        
        query = """
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large', %s
        )
        """
        
        cursor.execute(query, (event_prompt,))
        result = cursor.fetchone()
        return result[0] if result and result[0] else text
    except Exception as e:
        logger.warning(f"Error structuring event data with Snowflake Cortex: {str(e)}")
        return text

def get_embedding(text, cursor):
    """Get text embedding from Snowflake."""
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

def classify_event_using_snowflake_cortex(title, description, address, dates, cursor):
    """Classify event using Snowflake Cortex model."""
    try:
        event_details = f"""
        You will be provided with event details, and your task is to classify the event into one or more categories from the following list:
        Concert, Sports, Festival, Exhibition, Theater, Comedy Show, Food & Drink, Networking, Educational, Family-Friendly, Tech Conference, Other.

        Event Details:
        - Title: {title}
        - Description: {description}
        - Address: {address}
        - Event Dates: {dates}

        Please classify this event into one or more categories from the list above and return the category names as a list. The output should only be the categories strictly no other description or reasons.
        """
        
        # Snowflake query to classify the event based on event details
        query = """
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large', %s
        )
        """
        
        # Execute the query with the event_details passed as parameter
        cursor.execute(query, (event_details,))
        result = cursor.fetchone()
        categories = result[0] if result else []
        return categories
    except Exception as e:
        logger.error(f"Error classifying event using Snowflake Cortex: {str(e)}")
        return []

def handle_missing_end_date(description, cursor):
    """Handle missing end date by sending the description to Snowflake Cortex."""
    try:
        query = f"""
        You will be provided with event details, and your task is to extract the End Time from the description in the following format: "YYYY-MM-DD HH:MM:SS".
        
        Event Description: {description}
        
        Please return only the End Time in the specified format. In case end date details are not Present return - Not Available
        """
        
        result = structure_event_data(query, cursor)
        return result
    except Exception as e:
        logger.error(f"Error extracting end date from description: {str(e)}")
        return "Not Available"  # Return "Not Available" if the end date is missing

def preprocess_text_column(text):
    """Preprocesses the text data by trimming spaces and capitalizing the first letter of each sentence."""
    if text:
        text = text.strip()
        text = re.sub(r'(\w)([A-Z])', r'\1 \2', text)  # Add space between capital letters in camel case
        return text.capitalize()
    return text

def classify_missing_categories(row, cursor):
    """Classify events where category is missing or 'Not Available'."""
    if row.get('Categories') == 'No Categories':  # If Categories is 'Not Available'
        try:
            # Extract the necessary fields
            title = row['Event_Title']
            description = row['Description']
            address = row['Full_Address']
            dates = row['Start_Time']  # Assuming `Start_Time` holds event date
            
            # Classify the event using Snowflake Cortex
            categories = classify_event_using_snowflake_cortex(
                title, description, address, dates, cursor)
            
            # Log the categorization
            logger.info(f"Categorized event {title} with categories: {categories}")
            
            # Return the categories as a JSON string or a list
            return json.dumps(categories) if isinstance(categories, list) else str(categories)
        except Exception as e:
            logger.error(f"Error classifying event with 'Not Available' category: {str(e)}")
            return 'Not Available'
    return str(row['Categories']) if row['Categories'] is not None else None

def parallelize_structuring_and_embedding(df, cursor):
    """Parallelize structuring, categorizing, and embedding tasks."""
    def process_row(item):
        index, row = item  # Unpack the tuple from iterrows()
        
        # Combine row data into a single string for structuring
        combined_text = " | ".join([f"{col}: {row[col]}" for col in df.columns if col not in ['S3_URL', 'Event_URL', 'Image_URL']])
        
        # Step 1: Structure the event data using Snowflake Cortex
        structured_text = structure_event_data(combined_text, cursor)
        logger.info(f"Structured event {row['Event_Title']}")
        
        # Step 2: Get the vector embedding for the structured event
        embedding = get_embedding(structured_text, cursor)
        logger.info(f"Vectorized event {row['Event_Title']} with embedding.")
        
        return structured_text, embedding

    # Use ThreadPoolExecutor for parallel execution
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(process_row, df.iterrows()))

    # Unpack results
    structured_texts, embeddings = zip(*results)

    # Assign results back to DataFrame
    df['Structured_Text'] = structured_texts
    df['Vector_Embedding'] = embeddings
    
    return df

def load_to_edw(**context):
    """Process events data and load to Snowflake EDW table."""
    try:
        # Read event data
        ti = context['ti']
        events_file = ti.xcom_pull(task_ids='load_to_staging', key='events_staged')
        
        with open(events_file, 'r') as f:
            records = json.load(f)
        
        # Convert to DataFrame
        df = pd.DataFrame(records)
        
        # Get Snowflake connection
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Preprocess text columns
        df['Event_Title'] = df['Event_Title'].apply(preprocess_text_column)
        df['Description'] = df['Description'].apply(preprocess_text_column)
        df['Full_Address'] = df['Full_Address'].apply(preprocess_text_column)
        
        # Handle missing End_Date by sending description to Snowflake Cortex
        df['End_Time'] = df.apply(lambda row: handle_missing_end_date(row['Description'], cursor) if not row['End_Time'] else row['End_Time'], axis=1)
        
        # Classify events where category is missing or 'Not Available'
        df['Categories'] = df.apply(lambda row: classify_missing_categories(row, cursor), axis=1)
        
        # Parallelize the event structuring and embedding processes
        df = parallelize_structuring_and_embedding(df, cursor)
        
        # Add other columns like Source_Website and Date_Load_Time
        df["Source_Website"] = WEBSITE_NAME
        df["Date_Load_Time"] = pd.to_datetime("now")
        df.columns = [col.upper() for col in df.columns]
        
        # Log the DataFrame before inserting
        logger.info(f"DataFrame ready for insertion: {df.head()}")
        
        # Write DataFrame to Snowflake
        write_pandas(conn, df, EDW_TABLE)
        logger.info("Data inserted into Snowflake EDW table.")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error processing and loading to EDW: {str(e)}")
        raise
