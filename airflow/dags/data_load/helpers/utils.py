import re
import json
import logging
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import uuid
import requests
from io import BytesIO

logger = logging.getLogger(__name__)


def upload_event_image_to_s3(event, s3_client, bucket, s3_prefix, logger=None):
    """
    Downloads an image from an event and uploads it to S3.
    Updates the event dict with the S3 URL if successful.

    Args:
        event (dict): The event data.
        s3_client (boto3.client): The S3 client.
        bucket (str): Target S3 bucket.
        s3_prefix (str): Prefix for the S3 path (e.g., "boston_gov/YYYY/MM/DD/").
        logger (Logger, optional): Logger for logging messages.

    Returns:
        dict: Updated event with 'S3_URL' if image upload succeeded.
    """
    image_url = event.get('Image_URL')

    if image_url is None or image_url == 'No Image' or not image_url.startswith('http'):
        return event  # skip if image is invalid

    try:
        response = requests.get(image_url, timeout=10)
        if response.status_code == 200:
            # Generate unique filename
            event_id = str(uuid.uuid4())
            content_type = response.headers.get('Content-Type', '')

            if 'jpeg' in content_type or 'jpg' in content_type:
                ext = 'jpg'
            elif 'png' in content_type:
                ext = 'png'
            elif 'gif' in content_type:
                ext = 'gif'
            else:
                ext = 'jpg'  # Default fallback

            s3_key = f"{s3_prefix}{event_id}.{ext}"

            s3_client.upload_fileobj(
                BytesIO(response.content),
                bucket,
                s3_key,
                ExtraArgs={'ContentType': content_type}
            )

            s3_url = f"s3://{bucket}/{s3_key}"
            event['S3_URL'] = s3_url

            if logger:
                logger.info(f"Uploaded image for event '{event.get('Event_Title', 'Unknown')}' to {s3_url}")

    except Exception as e:
        if logger:
            logger.warning(f"Error processing image for '{event.get('Event_Title', 'Unknown')}': {str(e)}")

    return event


def preprocess_text_column(text):
    if text:
        text = text.strip()
        text = re.sub(r'(\w)([A-Z])', r'\1 \2', text)
        return text.capitalize()
    return text

def handle_missing_end_date(description, cursor):
    try:
        prompt = f"""
        You will be provided with event details, and your task is to extract the End Time from the description in the following format: "YYYY-MM-DD HH:MM:SS".

        Event Description: {description}

        Please return only the End Time in the specified format. If not available, return - Not Available.
        """
        return structure_event_data(prompt, cursor)
    except Exception as e:
        logger.error(f"Error extracting end date: {str(e)}")
        return "Not Available"

def classify_event_using_cortex(title, description, address, dates, cursor):
    try:
        prompt = f"""
        You will be provided with event details, and your task is to classify the event into one or more categories from the following list:
        Concert, Sports, Festival, Exhibition, Theater, Comedy Show, Food & Drink, Networking, Educational, Family-Friendly, Tech Conference, Other.

        Event Details:
        - Title: {title}
        - Description: {description}
        - Address: {address}
        - Event Dates: {dates}

        Please classify this event into one or more categories from the list above and return the category names as a list.
        """
        query = "SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', %s)"
        cursor.execute(query, (prompt,))
        result = cursor.fetchone()
        return json.dumps(result[0]) if result else "Other"
    except Exception as e:
        logger.error(f"Error classifying event: {str(e)}")
        return "Other"

def structure_event_data(text, cursor):
    try:
        prompt = f"""
        You are an expert at organizing and formatting event information. Your task is to take raw event data and structure it in a clean, consistent format that highlights the most important details.

        Please structure the following event information in a clean, well-organized format. Make sure to highlight important details like event title, date, time, location, and description. Remove any duplicate or irrelevant information. The output should contain only the result with no other explanations or wordings.

        Event information:
        {text}
        """
        query = "SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', %s)"
        cursor.execute(query, (prompt,))
        result = cursor.fetchone()
        return result[0] if result and result[0] else text
    except Exception as e:
        logger.warning(f"Error structuring event data: {str(e)}")
        return text

def get_embedding(text, cursor):
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

def parallelize_structuring_and_embedding(df, cursor):
    def process_row(item):
        index, row = item
        combined_text = " | ".join([f"{col}: {row[col]}" for col in df.columns if col not in ['S3_URL', 'EVENT_URL', 'IMAGE_URL']])
        structured = structure_event_data(combined_text, cursor)
        embedding = get_embedding(structured, cursor)
        return structured, embedding

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(process_row, df.iterrows()))
    structured_texts, embeddings = zip(*results)
    df['STRUCTURED_TEXT'] = structured_texts
    df['VECTOR_EMBEDDING'] = embeddings
    return df

def is_event_unique(embedding, cursor, threshold=0.9):
    try:
        query = """
        SELECT VECTOR_COSINE_SIMILARITY(
          VECTOR_EMBEDDING::VECTOR(FLOAT, 1024),
          PARSE_JSON(%s)::VECTOR(FLOAT, 1024)
        ) AS similarity
        FROM EDW.FACT_EVENTS_DETAILS
        ORDER BY similarity DESC
        LIMIT 1
        """
        cursor.execute(query, (json.dumps(embedding),))
        result = cursor.fetchone()
        max_similarity = float(result[0]) if result and result[0] is not None else 0
        return max_similarity < threshold
    except Exception as e:
        logger.error(f"Error checking vector similarity: {str(e)}")
        return True
