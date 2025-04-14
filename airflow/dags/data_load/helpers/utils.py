import re
import json
import logging
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import uuid
import requests
from io import BytesIO
import requests
import boto3
import base64
from openai import OpenAI
import tempfile
import cv2
from data_load.parameters.parameter_config import OPENAI_API_KEY,AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
import os

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

        Please classify this event into one or more categories from the list above and return the category names as a list. Strictly no other text or decription except the categories
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



def classify_event_into_group(title, description, location, categories, cursor):
    """
    Classifies an event into one or more of 10 defined categories using Snowflake Cortex.

    Args:
        title (str): Title of the event.
        description (str): Description text.
        location (str): Address or venue.
        details (str): Additional structured metadata (timing, tags, etc.).
        cursor: Snowflake DB cursor object.

    Returns:
        list: Predicted categories (one or more from the defined list).
    """
    try:
        prompt = f"""
You are an AI assistant helping categorize public events into meaningful groups.
Each event can belong to **one or more** of the following 10 categories. Below is what each category means:

1. **Arts & Culture** – Art, Exhibitions, Shows, Theater, Museums, History, Photography, Movies
2. **Food & Drink** – Food, Drinks, Cooking, Tastings, Culinary, Brewery, Wine, Coffee
3. **Nightlife & Parties** – Nightlife, Bars, Music, DJ, Clubbing, Party, Comedy Show
4. **Date Ideas** – Date Idea, Rainy Day Ideas, Romantic, Photoworthy, Couples
5. **Kids & Families** – Kid Friendly, Animals, Farms, Nature, Seasonal, Accessible Spots
6. **Professional & Networking / Educational** – Business & Professional, Networking, Jobs, Civic Engagement, Classes, Lectures & Conferences, University, Innovation, Virtual, Tech
7. **Sports & Active Life** – Sports & Active Life, Games, Fitness, Outdoor, Hiking
8. **Community & Social Good** – LGBTQ+, Social Good, Civic, Volunteering, Pet Friendly
9. **Fairs, Festivals & Shopping** – Festivals & Fairs, Markets, Shopping, Local Vendors, Seasonal
10. **Other** – Events that do not fit any of the above categories

Return only the category names (e.g., ["Food & Drink", "Nightlife & Parties"]) without any explanation or extra formatting or description strictly. The Output should be only like ["Food & Drink", "Nightlife & Parties"]

Event Details:
- Title: {title}
- Description: {description}
- Location: {location}
- Additional Info: {categories}
"""

        query = """
        SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', %s)
        """

        cursor.execute(query, (prompt,))
        result = cursor.fetchone()

        if result and result[0]:
            try:
                categories = json.loads(result[0])
                if isinstance(categories, list):
                    return categories
            except Exception:
                return [result[0].strip()]
        return ["Other"]

    except Exception as e:
        print(f"Error classifying event: {str(e)}")
        return ["Other"]


def extract_end_date_from_occurrence(occurrence_text: str, cursor, fallback_end_date: str) -> str:
    """
    Use Snowflake Cortex to extract the final end date from a recurrence string.

    Returns a date in the format 'Month DD, YYYY'.
    If no date is found, Cortex says 'Not Available' and we fallback to the existing END_DATE.
    """
    try:
        prompt = f"""
        You are a scheduling expert. From the following recurring event description,
        extract the **final end date** in the format: 'Month DD, YYYY' (e.g., July 20, 2025).

        If the description does not mention an explicit end date, say 'Not Available'. Strictly no other explanation or description.

        Recurrence Pattern:
        {occurrence_text}
        """

        cortex_query = "SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', %s)"
        cursor.execute(cortex_query, (prompt,))
        result = cursor.fetchone()

        if result and result[0]:
            response = result[0].strip()

            # Check for 'Not Available'
            if response.lower() == "not available":
                return fallback_end_date

            # Match something like 'July 20, 2025'
            match = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2}, \d{4}", response)
            if match:
               return match.group(0)

        # If response is bad or no match
        return fallback_end_date

    except Exception as e:
        print(f"Error extracting end date from occurrence: {e}")
        return fallback_end_date




def extract_event_fields_from_combined_text(combined_description, cursor):
    """
    Use Snowflake Cortex to extract structured fields from a combined event description.
    If a field cannot be confidently inferred, return 'Not Available' for that field.

    Args:
        combined_description (str): Merged event and media description
        cursor: Snowflake cursor to execute the Cortex query

    Returns:
        dict: Dictionary with keys:
              ['EVENT_TITLE', 'START_DATE', 'END_DATE', 'START_TIME', 'END_TIME',
               'LOCATION', 'FULL_ADDRESS', 'CATEGORIES', 'ADMISSION']
    """
    prompt = f"""
You are an expert event parser. From the combined event description below — which may contain information from social media posts, image descriptions, and video content — extract the following fields.

If any field is missing or cannot be confidently inferred, return its value as "Not Available".

Return ONLY a valid JSON object in the format below. **No explanation or description should be included. Strictly return only the JSON.**

{{
  "EVENT_TITLE": "",
  "START_DATE": "",
  "END_DATE": "",
  "START_TIME": "",
  "END_TIME": "",
  "LOCATION": "",
  "FULL_ADDRESS": "",
  "CATEGORIES": "",
  "ADMISSION": ""
}}

Date Format: Month DD, YYYY (e.g., July 20, 2025)  
Time Format: HH:MMAM/PM (e.g., 08:00PM)

Combined Event Description:
\"\"\"{combined_description}\"\"\"
"""

    try:
        query = "SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', %s)"
        cursor.execute(query, (prompt,))
        result = cursor.fetchone()
        if result and result[0]:
            parsed = json.loads(result[0])
            return parsed
    except Exception as e:
        print(f"Error extracting fields from combined description: {str(e)}")
    
    # Fallback values
    return {
        "EVENT_TITLE": "Not Available",
        "START_DATE": "Not Available",
        "END_DATE": "Not Available",
        "START_TIME": "Not Available",
        "END_TIME": "Not Available",
        "LOCATION": "Not Available",
        "FULL_ADDRESS": "Not Available",
        "CATEGORIES": "Not Available",
        "ADMISSION": "Not Available"
    }



def download_file(url, logger=None):
    """
    Download file from S3 or HTTP.
    """
    try:
        if url.startswith('s3://'):
            parts = url.replace('s3://', '').split('/', 1)
            bucket = parts[0]
            key = parts[1]
            s3_client = boto3.client('s3')
            response = s3_client.get_object(Bucket=bucket, Key=key)
            return response['Body'].read()

        elif url.startswith('http'):
            response = requests.get(url)
            response.raise_for_status()
            return response.content

        else:
            if logger:
                logger.error(f"Unsupported URL scheme: {url}")
            return None

    except Exception as e:
        if logger:
            logger.error(f"Error downloading file from {url}: {str(e)}")
        return None



def describe_image(image_url):
    """
    Describe an image using OpenAI Vision via direct image URL.

    Args:
        image_url (str): URL of the image (must be a valid HTTP/HTTPS link)

    Returns:
        str: Description generated by OpenAI Vision
    """
    try:
        if not OPENAI_API_KEY:
            return "No image description (API key not provided)"
        
        if not image_url or not image_url.startswith("http"):
            return "Invalid or missing image URL"

        client = OpenAI(api_key=OPENAI_API_KEY)

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "You will be provided with an image related to an event. Describe the image concisely, focusing only on event-specific details. If the image includes information such as the event name, timing, date, or price, extract and provide those details. Exclude any unrelated observations or interpretations. No other explanation or description strictly."
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": image_url
                            }
                        }
                    ]
                }
            ]
        )

        return response.choices[0].message.content

    except Exception as e:
        return f"Image description unavailable - Error: {str(e)}"


def download_video_file(url: str) -> bytes:
    """
    Downloads a video from a given URL (HTTP or S3) using credentials from parameter_config.
    
    Args:
        url (str): S3 or HTTP URL
        
    Returns:
        bytes: Raw video file content
    """
    try:
        if url.startswith("s3://"):
            parts = url.replace("s3://", "").split("/", 1)
            bucket, key = parts[0], parts[1]

            print(f"Downloading from S3 bucket: {bucket}, key: {key}")

            s3_client = boto3.client(
                "s3",
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )

            response = s3_client.get_object(Bucket=bucket, Key=key)
            print("S3 download successful.")
            return response["Body"].read()

        elif url.startswith("http"):
            print(f"Downloading from HTTP URL: {url}")
            response = requests.get(url)
            response.raise_for_status()
            print("HTTP download successful.")
            return response.content

        print("Unsupported URL scheme.")
        return None

    except Exception as e:
        print(f"Download error: {e}")
        return None


def extract_video_frames_base64(video_content: bytes):
    """
    Extracts all frames from the video and returns base64 encoded JPEG images.

    Args:
        video_content (bytes): Raw video content

    Returns:
        List[str]: List of base64-encoded JPEG frame strings
    """
    try:
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as temp_file:
            temp_file.write(video_content)
            temp_path = temp_file.name

        print(f"Temporary video written to {temp_path}")

        video = cv2.VideoCapture(temp_path)
        base64Frames = []
        frame_count = 0

        while video.isOpened():
            success, frame = video.read()
            if not success:
                break
            _, buffer = cv2.imencode(".jpg", frame)
            base64Frames.append(base64.b64encode(buffer).decode("utf-8"))
            frame_count += 1

        video.release()
        os.unlink(temp_path)

        print(f"{frame_count} frames extracted and encoded.")
        return base64Frames

    except Exception as e:
        print(f"Frame extraction error: {e}")
        return []

def describe_video_from_url(video_url: str) -> str:
    """
    Uses OpenAI to describe a video by analyzing evenly spaced frames (up to 6).
    """
    try:
        if not OPENAI_API_KEY:
            return "No video description (API key not provided)"

        print(f"Fetching video from: {video_url}")
        video_content = download_video_file(video_url)
        if not video_content:
            return "No video description (unable to download video)"

        print("Extracting evenly spaced frames...")
        frames = extract_video_frames_base64(video_content, frames_to_extract=6)
        if not frames:
            return "No video description (could not extract frames)"

        print(f"{len(frames)} frames selected for OpenAI")

        client = OpenAI(api_key=OPENAI_API_KEY)

        messages = [{
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": (
                        "You will be provided with still frames from a video related to an event. "
                        "Describe what you see in the video strictly focusing on event-specific content. "
                        "Include event type, date, time, or price if visible. "
                        "No other explanation or description strictly."
                    )
                }
            ] + [
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/jpeg;base64,{frame}"}
                } for frame in frames
            ]
        }]

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            max_tokens=300
        )

        print("OpenAI response received.")
        return response.choices[0].message.content.strip()

    except Exception as e:
        return f"Video description unavailable - Error: {str(e)}"
