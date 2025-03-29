"""
Script to process images from scraped data and store them in S3
"""
import os
import json
import requests
import boto3
from io import BytesIO
import uuid
import logging
from datetime import datetime
from data_load.parameters.parameter_config import (
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION,
    S3_BUCKET, TEMP_DIR
)

# Website specific settings - defined in the script
WEBSITE_NAME = "boston_calendar"

# Configure logging
logger = logging.getLogger(__name__)

def process_images(**context):
    """
    Download images from event URLs and upload them to S3.
    Updates the image URLs in the data to point to S3.
    """
    try:
        # Get the path to events data from the previous task
        ti = context['ti']
        events_file = ti.xcom_pull(task_ids=f'scrape_{WEBSITE_NAME}', key='events_data')
        
        # Create S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        
        # Read scraped events from file
        with open(events_file, 'r') as f:
            events = json.load(f)
        
        # Create folder structure in S3: website_name/YYYY/MM/DD/
        today = datetime.now().strftime('%Y/%m/%d')
        s3_prefix = f"{WEBSITE_NAME}/{today}/"
        
        # Process each event
        for event in events:
            image_url = event.get('Image_URL')
            
            # Skip if no image or invalid URL
            if image_url is None or image_url == 'No Image' or not image_url.startswith('http'):
                continue
                
            try:
                # Download image
                response = requests.get(image_url, timeout=10)
                
                if response.status_code == 200:
                    # Generate a unique filename
                    event_id = str(uuid.uuid4())
                    
                    # Get file extension from content type
                    content_type = response.headers.get('Content-Type', '')
                    if 'jpeg' in content_type or 'jpg' in content_type:
                        ext = 'jpg'
                    elif 'png' in content_type:
                        ext = 'png'
                    elif 'gif' in content_type:
                        ext = 'gif'
                    else:
                        ext = 'jpg'  # Default to jpg
                    
                    # Create S3 key
                    s3_key = f"{s3_prefix}{event_id}.{ext}"
                    
                    # Upload to S3
                    s3_client.upload_fileobj(
                        BytesIO(response.content),
                        S3_BUCKET,
                        s3_key,
                        ExtraArgs={'ContentType': response.headers.get('Content-Type')}
                    )
                    
                    # Update S3_URL in event data while preserving original image_url
                    s3_url = f"s3://{S3_BUCKET}/{s3_key}"
                    event['S3_URL'] = s3_url
                    
                    logger.info(f"Uploaded image for event '{event['Event_Title']}' to {s3_url}")
                    
            except Exception as img_error:
                logger.warning(f"Error processing image for '{event['Event_Title']}': {str(img_error)}")
                # Keep the original URL if there's an error
        
        # Get the site-specific temp directory
        site_temp_dir = os.path.dirname(events_file)
        
        # Save updated events to file
        output_file = os.path.join(site_temp_dir, 'events_with_s3_images.json')
        with open(output_file, 'w') as f:
            json.dump(events, f)
            
        logger.info(f"Processed images for {len(events)} events and saved to {output_file}")
        
        # Pass the updated file path to the next task
        context['ti'].xcom_push(key='events_with_images', value=output_file)
        
    except Exception as e:
        logger.error(f"Error in process_images: {str(e)}")
        raise