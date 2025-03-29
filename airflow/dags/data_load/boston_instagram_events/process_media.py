"""
Script to process images and videos from Instagram posts and upload to S3
"""
import os
import json
import requests
import boto3
import cv2
import tempfile
import numpy as np
from io import BytesIO
import uuid
import logging
from datetime import datetime
from data_load.parameters.parameter_config import (
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION,
    S3_BUCKET, TEMP_DIR
)

# Website specific settings - defined in the script
WEBSITE_NAME = "instagram_events"

# Configure logging
logger = logging.getLogger(__name__)

def download_file(url, timeout=30):
    """
    Download file from URL
    
    Args:
        url (str): URL to download
        timeout (int): Timeout in seconds
        
    Returns:
        bytes: File content
    """
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response.content
    except Exception as e:
        logger.error(f"Error downloading file from {url}: {str(e)}")
        return None

def extract_video_frame(video_content, frame_position=0.25):
    """
    Extract a frame from a video at the specified position
    
    Args:
        video_content (bytes): Video content
        frame_position (float): Position in the video (0.0 to 1.0)
        
    Returns:
        bytes: JPG frame content
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
            return None
        
        # Calculate frame position
        frame_pos = int(total_frames * frame_position)
        
        # Set frame position
        cap.set(cv2.CAP_PROP_POS_FRAMES, frame_pos)
        
        # Read frame
        success, frame = cap.read()
        
        # Release video
        cap.release()
        
        # Delete temporary file
        os.unlink(temp_video_path)
        
        if not success:
            logger.error("Failed to read frame from video")
            return None
        
        # Encode frame to JPG
        _, buffer = cv2.imencode('.jpg', frame)
        return buffer.tobytes()
        
    except Exception as e:
        logger.error(f"Error extracting frame from video: {str(e)}")
        # Clean up temp file if it exists
        try:
            os.unlink(temp_video_path)
        except:
            pass
        return None

def process_media(**context):
    """
    Download images and videos from Instagram posts and upload them to S3.
    Updates the URLs in the data to point to S3.
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
        s3_prefix = f"{WEBSITE_NAME}/"
        
        # Process each event
        for event in events:
            # Process image
            image_url = event.get('Image_URL')
            
            if image_url is not None and image_url != 'No Image' and isinstance(image_url, str) and image_url.startswith('http'):
                try:
                    # Download image
                    image_content = download_file(image_url)
                    
                    if image_content:
                        # Generate a unique filename
                        event_id = str(uuid.uuid4())
                        
                        # Create S3 key for image
                        s3_key = f"{s3_prefix}{event_id}_image.jpg"
                        
                        # Upload to S3
                        s3_client.upload_fileobj(
                            BytesIO(image_content),
                            S3_BUCKET,
                            s3_key,
                            ExtraArgs={'ContentType': 'image/jpeg'}
                        )
                        
                        # Update image URL in event data
                        s3_url = f"s3://{S3_BUCKET}/{s3_key}"
                        event['S3_Image_URL'] = s3_url
                        
                        logger.info(f"Uploaded image to {s3_url}")
                except Exception as e:
                    logger.warning(f"Error processing image: {str(e)}")
            
            # Process video
            video_url = event.get('Video_URL')
            
            if video_url is not None and isinstance(video_url, str) and video_url.startswith('http'):
                try:
                    # Download video
                    video_content = download_file(video_url)
                    
                    if video_content:
                        # Generate a unique filename
                        event_id = str(uuid.uuid4())
                        
                        # Create S3 key for video
                        video_s3_key = f"{s3_prefix}{event_id}_video.mp4"
                        
                        # Upload video to S3
                        s3_client.upload_fileobj(
                            BytesIO(video_content),
                            S3_BUCKET,
                            video_s3_key,
                            ExtraArgs={'ContentType': 'video/mp4'}
                        )
                        
                        # Update video URL in event data
                        video_s3_url = f"s3://{S3_BUCKET}/{video_s3_key}"
                        event['S3_Video_URL'] = video_s3_url
                        
                        logger.info(f"Uploaded video to {video_s3_url}")
                        
                        # Extract a frame for thumbnail
                        frame_content = extract_video_frame(video_content)
                        
                        if frame_content:
                            # Create S3 key for thumbnail
                            thumb_s3_key = f"{s3_prefix}{event_id}_thumbnail.jpg"
                            
                            # Upload thumbnail to S3
                            s3_client.upload_fileobj(
                                BytesIO(frame_content),
                                S3_BUCKET,
                                thumb_s3_key,
                                ExtraArgs={'ContentType': 'image/jpeg'}
                            )
                            
                            # Update thumbnail URL in event data
                            thumb_s3_url = f"s3://{S3_BUCKET}/{thumb_s3_key}"
                            event['S3_Thumbnail_URL'] = thumb_s3_url
                            
                            logger.info(f"Uploaded video thumbnail to {thumb_s3_url}")
                except Exception as e:
                    logger.warning(f"Error processing video: {str(e)}")
        
        # Get the site-specific temp directory
        site_temp_dir = os.path.dirname(events_file)
        
        # Save updated events to file
        output_file = os.path.join(site_temp_dir, 'events_with_s3_media.json')
        with open(output_file, 'w') as f:
            json.dump(events, f)
            
        logger.info(f"Processed media for {len(events)} events and saved to {output_file}")
        
        # Pass the updated file path to the next task
        context['ti'].xcom_push(key='events_with_media', value=output_file)
        
    except Exception as e:
        logger.error(f"Error in process_media: {str(e)}")
        raise