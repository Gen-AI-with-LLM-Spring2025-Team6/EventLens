"""
Script to process Instagram data and load to Snowflake EDW
"""
import os
import json
import pandas as pd
import snowflake.connector
import tempfile
import cv2
import base64
from snowflake.connector.pandas_tools import write_pandas
from openai import OpenAI
import requests
import logging
from datetime import datetime
from data_load.parameters.parameter_config import (
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE,
    OPENAI_API_KEY
)

# Website specific settings - defined in the script
WEBSITE_NAME = "instagram_events"
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

def download_file(url):
    """
    Download file from S3 or HTTP URL
    
    Args:
        url (str): URL to download
        
    Returns:
        bytes: File content
    """
    try:
        if url.startswith('s3://'):
            # Parse S3 URL
            parts = url.replace('s3://', '').split('/', 1)
            bucket = parts[0]
            key = parts[1]
            
            # Download from S3
            import boto3
            s3_client = boto3.client('s3')
            response = s3_client.get_object(Bucket=bucket, Key=key)
            return response['Body'].read()
        elif url.startswith('http'):
            # Download from HTTP
            response = requests.get(url)
            response.raise_for_status()
            return response.content
        else:
            logger.error(f"Unsupported URL scheme: {url}")
            return None
    except Exception as e:
        logger.error(f"Error downloading file from {url}: {str(e)}")
        return None

def extract_video_frames(video_content, frames_to_extract=10):
    """
    Extract frames from a video
    
    Args:
        video_content (bytes): Video content
        frames_to_extract (int): Number of frames to extract
        
    Returns:
        list: List of base64 encoded frames
    """
    try:
        # Create a temporary file for the video
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_video:
            temp_video.write(video_content)
            temp_video_path = temp_video.name
        
        # Open the video file
        cap = cv2.VideoCapture(temp_video_path)
        
        # Get total frames
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        
        if total_frames <= 0:
            logger.error("Video has no frames")
            os.unlink(temp_video_path)
            return []
        
        # Calculate frame interval
        interval = max(1, total_frames // frames_to_extract)
        
        # Extract frames
        base64_frames = []
        for i in range(0, total_frames, interval):
            if len(base64_frames) >= frames_to_extract:
                break
                
            # Set frame position
            cap.set(cv2.CAP_PROP_POS_FRAMES, i)
            
            # Read frame
            success, frame = cap.read()
            
            if not success:
                continue
                
            # Encode frame to JPG
            _, buffer = cv2.imencode('.jpg', frame)
            # Convert to base64
            base64_frame = base64.b64encode(buffer).decode('utf-8')
            base64_frames.append(base64_frame)
        
        # Release video
        cap.release()
        
        # Delete temporary file
        os.unlink(temp_video_path)
        
        return base64_frames
        
    except Exception as e:
        logger.error(f"Error extracting frames from video: {str(e)}")
        # Clean up temp file if it exists
        try:
            os.unlink(temp_video_path)
        except:
            pass
        return []

def describe_image(image_content):
    """
    Describe an image using OpenAI Vision
    
    Args:
        image_content (bytes): Image content
        
    Returns:
        str: Image description
    """
    try:
        # Skip if no API key
        if not OPENAI_API_KEY:
            return "No image description (API key not provided)"
            
        # Convert image to base64
        base64_image = base64.b64encode(image_content).decode('utf-8')
        
        # Create OpenAI client
        client = OpenAI(api_key=OPENAI_API_KEY)
        
        # Call OpenAI API
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "You will be provided with an image related to an event. Describe the image concisely, focusing only on event-specific details. If the image includes information such as the event name, timing, date, or price, extract and provide those details. Exclude any unrelated observations or interpretations.",
                        },
                        {
                            "type": "image_url",
                            "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"},
                        },
                    ],
                }
            ],
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        logger.error(f"Error describing image: {str(e)}")
        return f"Image description unavailable - Error: {str(e)}"

def describe_video(base64_frames):
    """
    Describe a video using OpenAI Vision
    
    Args:
        base64_frames (list): List of base64 encoded frames
        
    Returns:
        str: Video description
    """
    try:
        # Skip if no API key
        if not OPENAI_API_KEY:
            return "No video description (API key not provided)"
            
        # Skip if no frames
        if not base64_frames:
            return "No video description (no frames)"
            
        # Create OpenAI client
        client = OpenAI(api_key=OPENAI_API_KEY)
        
        # Prepare messages
        content = [
            "I am uploading frames extracted from a video. Provide a detailed description of what you see in the video, focusing only on the content. Identify the type of event depicted and include relevant details such as event type, timing, and pricing if they appear in the frames. Ensure the response is purely descriptive without additional interpretation or assumptions."
        ]
        
        # Add frames to content (every other frame to stay within token limits)
        for frame in base64_frames[::2]:
            content.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{frame}"}
            })
        
        # Call OpenAI API
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{
                "role": "user",
                "content": content
            }],
            max_tokens=300
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        logger.error(f"Error describing video: {str(e)}")
        return f"Video description unavailable - Error: {str(e)}"

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
                    "content": f"Please structure the following Instagram event information in a clean, well-organized format. Make sure to highlight important details like event title, date, time, location, and description. Remove any duplicate or irrelevant information:\n\n{text}"
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
    Process Instagram events data and load to Snowflake EDW table
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
        
        # Process each record for media descriptions
        for record in records:
            # Process image if available
            image_description = "No image available"
            if 'S3_Image_URL' in record and record['S3_Image_URL']:
                try:
                    # Download image from S3
                    image_content = download_file(record['S3_Image_URL'])
                    if image_content:
                        # Describe image
                        image_description = describe_image(image_content)
                        record['Image_Description'] = image_description
                except Exception as e:
                    logger.error(f"Error processing image for record: {str(e)}")
            
            # Process video if available
            video_description = "No video available"
            if 'S3_Video_URL' in record and record['S3_Video_URL']:
                try:
                    # Download video from S3
                    video_content = download_file(record['S3_Video_URL'])
                    if video_content:
                        # Extract frames
                        frames = extract_video_frames(video_content, frames_to_extract=5)
                        # Describe video
                        if frames:
                            video_description = describe_video(frames)
                            record['Video_Description'] = video_description
                except Exception as e:
                    logger.error(f"Error processing video for record: {str(e)}")
            
            # Combine descriptions with the existing description
            if 'Image_Description' in record and 'Video_Description' in record:
                media_description = f"Image: {record['Image_Description']}\n\nVideo: {record['Video_Description']}"
            elif 'Image_Description' in record:
                media_description = record['Image_Description']
            elif 'Video_Description' in record:
                media_description = record['Video_Description']
            else:
                media_description = "No media description available"
                
            # Append media description to existing description
            if 'Description' in record and record['Description'] and record['Description'] != 'No Description':
                record['Description'] = f"{record['Description']}\n\n--- Media Analysis ---\n{media_description}"
            else:
                record['Description'] = media_description
        
        # Convert to DataFrame
        df = pd.DataFrame(records)
        
        # Function to combine all columns into a single text representation
        def combine_row_data(row):
            # Include Instagram specific fields
            text_parts = []
            for col in df.columns:
                if pd.notna(row[col]) and row[col] not in (None, ''):
                    # Skip URLs and other non-textual fields
                    if col not in ('S3_Image_URL', 'S3_Video_URL', 'S3_Thumbnail_URL', 'Image_URL', 'Video_URL'):
                        text_parts.append(f"{col}: {row[col]}")
            
            return " | ".join(text_parts)
            
        # Apply function to all rows
        df["Combined_Text"] = df.apply(combine_row_data, axis=1)
        
        # Structure and clean the combined text using OpenAI
        logger.info("Structuring Instagram event data using OpenAI...")
        df["Structured_Text"] = df["Combined_Text"].apply(structure_event_data)
        
        # Generate embeddings for each row using Snowflake Arctic
        logger.info("Generating vector embeddings...")
        df["Vector_Embedding"] = df["Structured_Text"].apply(lambda text: get_embedding(text, cursor))
        
        # Add additional columns
        df["Source_Website"] = WEBSITE_NAME
        df["Date_Load_Time"] = pd.to_datetime("now")
        
        # Rename columns to match Snowflake schema (uppercase)
        df.columns = df.columns.str.upper()
        
        # Prepare DataFrame for writing to Snowflake
        # Select only columns present in the EDW table schema
        edw_columns = [
            "EVENT_TITLE", "IMAGE_URL", "S3_URL", "START_TIME", "END_TIME", "LOCATION",
            "FULL_ADDRESS", "CATEGORIES", "ADMISSION", "DESCRIPTION", "EVENT_URL", 
            "COMBINED_TEXT", "STRUCTURED_TEXT", "VECTOR_EMBEDDING", 
            "SOURCE_WEBSITE", "DATE_LOAD_TIME"
        ]
        
        # Map Instagram columns to EDW columns
        df_edw = pd.DataFrame()
        df_edw["EVENT_TITLE"] = df["EVENT_TITLE"]
        df_edw["IMAGE_URL"] = df["IMAGE_URL"]
        df_edw["S3_URL"] = df["S3_IMAGE_URL"].fillna(df["S3_VIDEO_URL"])
        df_edw["START_TIME"] = df["START_TIME"]
        df_edw["END_TIME"] = df["END_TIME"]
        df_edw["LOCATION"] = df["LOCATION"]
        df_edw["FULL_ADDRESS"] = df["FULL_ADDRESS"]
        df_edw["CATEGORIES"] = df["CATEGORIES"]
        df_edw["ADMISSION"] = df["ADMISSION"]
        df_edw["DESCRIPTION"] = df["DESCRIPTION"]  # Already includes the media description
        df_edw["EVENT_URL"] = df["EVENT_URL"]
        df_edw["COMBINED_TEXT"] = df["COMBINED_TEXT"]
        df_edw["STRUCTURED_TEXT"] = df["STRUCTURED_TEXT"]
        df_edw["VECTOR_EMBEDDING"] = df["VECTOR_EMBEDDING"]
        df_edw["SOURCE_WEBSITE"] = df["SOURCE_WEBSITE"]
        df_edw["DATE_LOAD_TIME"] = df["DATE_LOAD_TIME"]
        
        # Write to Snowflake
        logger.info(f"Loading Instagram data to {EDW_SCHEMA}.{EDW_TABLE}...")
        result = write_pandas(conn, df_edw, EDW_TABLE)
        
        logger.info(f"Data insertion to {EDW_SCHEMA}.{EDW_TABLE} result: {result}")
        
        # Close connection
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error processing and loading to EDW: {str(e)}")
        raise