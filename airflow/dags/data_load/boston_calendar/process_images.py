"""
Script to process images from scraped data and store them in S3
"""
import os
import json
import boto3
import logging
from datetime import datetime
from data_load.parameters.parameter_config import (
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION,
    S3_BUCKET, TEMP_DIR
)
from data_load.helpers.utils import upload_event_image_to_s3

# Website-specific setting
WEBSITE_NAME = "boston_calendar"

# Logger setup
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def process_images(**context):
    """
    Downloads images from event URLs and uploads them to S3.
    Updates the event data with the new S3 URL.
    """
    try:
        # Get file path from previous task
        ti = context['ti']
        events_file = ti.xcom_pull(task_ids=f'scrape_{WEBSITE_NAME}', key='events_data')

        # Read events from JSON file
        with open(events_file, 'r') as f:
            events = json.load(f)

        if not events:
            logger.warning("No events to process for image upload.")
            return

        # Setup S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

        # Generate S3 prefix based on today's date
        today = datetime.now().strftime('%Y/%m/%d')
        s3_prefix = f"{WEBSITE_NAME}/{today}/"

        # Upload each event's image
        updated_events = []
        for event in events:
            updated_event = upload_event_image_to_s3(event, s3_client, S3_BUCKET, s3_prefix, logger)
            updated_events.append(updated_event)

        # Write back updated event data
        site_temp_dir = os.path.dirname(events_file)
        output_file = os.path.join(site_temp_dir, 'events_with_s3_images.json')
        with open(output_file, 'w') as f:
            json.dump(updated_events, f)

        logger.info(f"Processed images for {len(updated_events)} events and saved to {output_file}")
        context['ti'].xcom_push(key='events_with_images', value=output_file)

    except Exception as e:
        logger.error(f"Error in process_images: {str(e)}")
        raise
