"""
Script to scrape events from Boston Calendar website
"""
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import logging
from data_load.parameters.parameter_config import TEMP_DIR
from datetime import datetime

# Website name - defined in the script
WEBSITE_NAME = "boston_calendar"

# Configure logging
logger = logging.getLogger(__name__)

def scrape_boston_calendar(**context):
    """
    Scrape events from Boston Calendar website and save to temp file
    
    Returns:
        DataFrame: Scraped events data
    """
    try:
        # Get today's date
        today = datetime.today()
        day = today.day
        month = today.month
        year = today.year
        
        # Construct the URL dynamically based on the current date
        URL = f"https://www.thebostoncalendar.com/events?day={day}&month={month}&week=1&year={year}"
        #URL ="https://www.thebostoncalendar.com/events?date=2025-05-10&day=29&month=5&year=2025"
        
        logger.info(f"Scraping events from {URL}")
        
        # Request headers
        HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        # Get HTML content
        response = requests.get(URL, headers=HEADERS)
        
        if response.status_code != 200:
            logger.error(f"Failed to fetch page: {response.status_code}")
            raise Exception(f"Failed to fetch page: {response.status_code}")
            
        # Convert to Soup object
        soup = BeautifulSoup(response.text, "lxml")
        
        # Find all events
        events = soup.find_all("li", class_="event")
        
        # Extract basic event information
        event_data = []
        for event in events:
            title_tag = event.find("h3").find("a")
            title = title_tag.get_text(strip=True) if title_tag else "No Title"

            link = title_tag["href"] if title_tag and title_tag.has_attr("href") else "No Link"
            full_link = f"https://www.thebostoncalendar.com{link}" if link.startswith("/") else link

            event_data.append({"Title": title, "Link": full_link})
            
        df = pd.DataFrame(event_data)
        
        # Process each event to get detailed information
        event_list = []
        for index, row in df.iterrows():
            EVENT_URL = row["Link"]
            
            # Get HTML content for each event
            event_response = requests.get(EVENT_URL, headers=HEADERS)
            
            if event_response.status_code == 200:
                event_soup = BeautifulSoup(event_response.text, "lxml")

                # Extract event title
                title_tag = event_soup.find("h1", itemprop="name")
                title = title_tag.get_text(strip=True) if title_tag else "No Title"

                # Extract event image URL
                image_tag = event_soup.find("a", class_="zoom_in")
                image_url = image_tag["href"] if image_tag and image_tag.has_attr("href") else "No Image"

                # Extract event time
                start_time_tag = event_soup.find("span", id="startdate", itemprop="startDate")
                end_time_tag = event_soup.find("span", id="startdate", itemprop="endDate")
                start_time = start_time_tag["content"] if start_time_tag and start_time_tag.has_attr("content") else "No Start Time"
                end_time = end_time_tag["content"] if end_time_tag and end_time_tag.has_attr("content") else "No End Time"

                # Extract event location
                location_name_tag = event_soup.find("span", itemprop="name")
                street_address_tag = event_soup.find("span", itemprop="streetAddress")
                city_tag = event_soup.find("span", itemprop="addressLocality")
                state_tag = event_soup.find("span", itemprop="addressRegion")
                postal_code_tag = event_soup.find("span", itemprop="postalCode")

                location_name = location_name_tag.get_text(strip=True) if location_name_tag else "No Location Name"
                street_address = street_address_tag.get_text(strip=True) if street_address_tag else "No Street Address"
                city = city_tag.get_text(strip=True) if city_tag else "No City"
                state = state_tag.get_text(strip=True) if state_tag else "No State"
                postal_code = postal_code_tag.get_text(strip=True) if postal_code_tag else "No Postal Code"
                full_address = f"{street_address}, {city}, {state} {postal_code}"

                # Extract event categories
                categories_tag = event_soup.find("b", string="Categories:")
                categories = categories_tag.find_next_sibling(string=True).strip() if categories_tag else "No Categories"

                # Extract event admission details
                admission_tag = event_soup.find("b", string="Admission:")
                admission = admission_tag.find_next_sibling("span").get_text(strip=True) if admission_tag else "No Admission Info"

                # Extract event description
                description_tag = event_soup.find("div", id="event_description")
                description = description_tag.get_text(strip=True) if description_tag else "No Description"

                # Skip if the description contains a link to the same site
                skip_event = False
                if description_tag:
                    links = description_tag.find_all("a", href=True)
                    for link in links:
                        if "https://www.thebostoncalendar.com/events/" in link["href"]:
                            skip_event = True
                            break

                if not skip_event:
                    event_data = {
                        "Event_Title": title,
                        "Image_URL": image_url,
                        "Start_Time": start_time,
                        "End_Time": end_time,
                        "Location": location_name,
                        "Full_Address": full_address,
                        "Categories": categories,
                        "Admission": admission,
                        "Description": description,
                        "Event_URL": EVENT_URL
                    }
                    event_list.append(event_data)

        # Convert list to DataFrame
        df_events = pd.DataFrame(event_list)
        #df_events = df_events.iloc[0:5]
        
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
        logger.error(f"Error scraping Boston Calendar: {str(e)}")
        raise
