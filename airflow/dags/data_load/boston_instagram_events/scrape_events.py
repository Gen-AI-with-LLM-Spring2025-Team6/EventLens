"""
Script to scrape events from Instagram using Apify
"""
import os
import json
import pandas as pd
import logging
from apify_client import ApifyClient
from data_load.parameters.parameter_config import TEMP_DIR, APIFY_API_KEY

# Website specific settings - defined in the script
WEBSITE_NAME = "instagram_events"

# Configure logging
logger = logging.getLogger(__name__)

def scrape_instagram_events(**context):
    """
    Scrape events from Instagram using Apify API and save to temp file
    
    Returns:
        DataFrame: Scraped events data
    """
    try:
        # Initialize the ApifyClient with your API token
        client = ApifyClient(APIFY_API_KEY)
        
        # Instagram accounts to scrape
        instagram_accounts = [
            "bostontoday",
            "bostondesievents",
            "bostoneventguide", 
            "bostonparksdept"
        ]
        
        direct_urls = [f"https://www.instagram.com/{account}/" for account in instagram_accounts]
        
        logger.info(f"Starting Instagram scrape for accounts: {', '.join(instagram_accounts)}")
        
        # Prepare the Actor input
        run_input = {
            "directUrls": direct_urls,
            "resultsType": "posts",
            "resultsLimit": 2,
            "searchType": "hashtag",
            "searchLimit": 1,
            "addParentData": False,
        }
        
        # Run the Actor and wait for it to finish
        logger.info("Running Apify actor for Instagram scraping")
        run = client.actor("shu8hvrXbJbY3Eb9W").call(run_input=run_input)
        
        # Collect the results
        data_list = []
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            data_list.append(item)
        
        # Convert to DataFrame
        df_instagram = pd.DataFrame(data_list)
        
        logger.info(f"Retrieved {len(df_instagram)} posts from Instagram")
        
        # Select and clean relevant columns
        selected_columns = [
            "inputUrl", "id", "type", "caption", "hashtags", "url", 
            "displayUrl", "images", "timestamp", "ownerUsername", 
            "videoUrl", "productType", "locationName"
        ]
        
        # Handle missing columns
        for col in selected_columns:
            if col not in df_instagram.columns:
                df_instagram[col] = None
                
        df_selected = df_instagram[selected_columns].copy()
        
        # Clean DataFrame - handle JSON serialization for lists and dicts
        for col in df_selected.columns:
            df_selected[col] = df_selected[col].apply(
                lambda x: str(x) if isinstance(x, (dict, list)) else x
            )
            df_selected[col] = df_selected[col].apply(
                lambda x: None if pd.isna(x) else x
            )
        
        # Map to our standard format
        event_list = []
        for _, row in df_selected.iterrows():
            event_data = {
                "Event_Title": f"Instagram Post by {row['ownerUsername']}" if pd.notna(row['ownerUsername']) else "Instagram Post",
                "Image_URL": row["displayUrl"] if pd.notna(row["displayUrl"]) else None,
                "Video_URL": row["videoUrl"] if pd.notna(row["videoUrl"]) else None,
                "Start_Time": row["timestamp"] if pd.notna(row["timestamp"]) else None,
                "End_Time": None,
                "Location": row["locationName"] if pd.notna(row["locationName"]) else "No Location",
                "Full_Address": row["locationName"] if pd.notna(row["locationName"]) else "No Location",
                "Categories": row["hashtags"] if pd.notna(row["hashtags"]) else "No Categories",
                "Admission": "Instagram Post",
                "Description": row["caption"] if pd.notna(row["caption"]) else "No Description",
                "Event_URL": row["url"] if pd.notna(row["url"]) else None,
                "Source_Account": row["ownerUsername"] if pd.notna(row["ownerUsername"]) else None,
                "Post_Type": row["type"] if pd.notna(row["type"]) else None,
                "Post_ID": row["id"] if pd.notna(row["id"]) else None
            }
            event_list.append(event_data)
        
        # Convert to DataFrame
        df_events = pd.DataFrame(event_list)
        
        # Create temp directory for this website
        site_temp_dir = os.path.join(TEMP_DIR, WEBSITE_NAME)
        os.makedirs(site_temp_dir, exist_ok=True)
        
        # Save DataFrame to file
        output_file = os.path.join(site_temp_dir, 'scraped_events.json')
        df_events.to_json(output_file, orient='records')
        
        logger.info(f"Successfully scraped {len(df_events)} events and saved to {output_file}")
        
        # Pass the file path to the next task via XCom
        context['ti'].xcom_push(key='events_data', value=output_file)
        
        return df_events
        
    except Exception as e:
        logger.error(f"Error scraping Instagram events: {str(e)}")
        raise