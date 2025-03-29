"""
Script to scrape events from Boston.gov website using Chromium
"""
import os
import re
import time
import pandas as pd
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
from data_load.parameters.parameter_config import TEMP_DIR

# Website specific settings - defined in the script
WEBSITE_NAME = "boston_gov"

# Configure logging
logger = logging.getLogger(__name__)

def scrape_boston_gov(**context):
    """
    Scrape events from Boston.gov website and save to temp file
    
    Returns:
        DataFrame: Scraped events data
    """
    try:
        # Setting up Chrome options for Chromium
        options = Options()
        options.add_argument("--headless")  # Run in headless mode
        options.add_argument("--no-sandbox")  # Required for running in Docker
        options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems
        options.add_argument("--disable-gpu")  # Disable GPU acceleration
        options.add_argument('--disable-software-rasterizer')
        options.add_argument("--disable-extensions")  # Disable extensions
        options.binary_location = "/usr/bin/chromium"  # Specify the Chromium binary location

        # Set up the service for Chromium
        service = Service(executable_path="/usr/bin/chromedriver")

        logger.info("Starting Chromium with explicit binary path and service path")
        
        # Initialize the driver with explicit service and options
        driver = webdriver.Chrome(service=service, options=options)
        
        # Home URL
        BASE_URL = "https://www.boston.gov/events?page="
        event_list = []
        
        # To Loop through the first 3 pages
        for page in range(3):
            url = BASE_URL + str(page)
            logger.info(f"Scraping page: {page + 1} -> {url}")
            
            # Load the page
            driver.get(url)
            
            # Wait for the page to load
            time.sleep(5)  # Simple waiting strategy
            
            # Get the page source
            page_source = driver.page_source
            
            # Parse with BeautifulSoup
            soup = BeautifulSoup(page_source, "html.parser")
            
            # Find all event articles
            articles = soup.find_all("article", class_="calendar-listing-wrapper")
            
            if not articles:
                logger.warning(f"No events found on page {page + 1}")
            else:
                logger.info(f"Found {len(articles)} events on page {page + 1}")
            
            # Extract event information
            for article in articles:
                title_tag = article.find("div", class_="title")
                title = title_tag.text.strip() if title_tag else "No Title"
                
                details_link_tag = article.find("a", class_="button")
                details_link = "https://www.boston.gov" + details_link_tag["href"] if details_link_tag and details_link_tag.has_attr("href") else "No Link"
                
                event_list.append({"Title": title, "Event Details Link": details_link})
            
            # Wait before moving to the next page
            time.sleep(3)
        
        # Close the driver after scraping the listing pages
        driver.quit()
        
        # If no events were found, log an error
        if not event_list:
            logger.error("No events found on Boston.gov")
            raise Exception("No events found on Boston.gov")
            
        logger.info(f"Found {len(event_list)} events on Boston.gov")
        
        # Convert to DataFrame
        df_events = pd.DataFrame(event_list)
        
        # Initialize a new driver for scraping individual event pages
        driver = webdriver.Chrome(service=service, options=options)
        
        event_details_list = []
        
        # Limit to 5 events for testing
        event_count = min(5, len(df_events))
        logger.info(f"Processing {event_count} events in detail")
        
        for index, row in df_events.head(event_count).iterrows():
            URL = row["Event Details Link"]
            
            # Skip invalid URLs
            if not re.match(r"^https://www\.boston\.gov/node/\d+$", URL):
                logger.info(f"Skipping invalid URL: {URL}")
                continue
                
            logger.info(f"Scraping: {URL}")
            
            try:
                # Load the event page
                driver.get(URL)
                
                # Simple waiting strategy
                time.sleep(5)
                
                # Get the page source
                page_source = driver.page_source
                
                # Parse with BeautifulSoup
                soup = BeautifulSoup(page_source, "html.parser")
                
                # Extract event details
                title_tag = soup.find("h1")
                title = title_tag.text.strip() if title_tag else "No Title"
                
                # Extract Start Date
                start_date_tag = soup.find("div", class_="date-title t--intro")
                start_date = start_date_tag.text.strip() if start_date_tag else "No Start Date"
                
                # Extract Event Timings
                timing_tag = soup.find("div", class_="detail-item__content")
                event_timing = timing_tag.get_text(strip=True, separator=" ") if timing_tag else "No Event Timings"
                
                # Extract Location
                location_tag = soup.find("div", class_="addr-a street-block")
                location = location_tag.text.strip() if location_tag else "No Location"
                
                # Extract Contact
                contact_tag = soup.find("div", class_="detail-item__body detail-item__body--secondary field-item")
                contact = contact_tag.text.strip() if contact_tag else "No Contact Info"
                
                # Extract Email
                email_tag = soup.find("a", href=lambda x: x and x.startswith("mailto:"))
                email = email_tag["href"].replace("mailto:", "") if email_tag else "No Email"
                
                # Extract Admission Price
                admission_price = "No Price Info"
                
                # Find admission information
                details_list = soup.find_all("li", class_="dl-i evt-sb-i")
                for detail in details_list:
                    label_tag = detail.find("div", class_="dl-t")
                    value_tag = detail.find("div", class_="detail-item__body--secondary field-item", role="listitem")
                    
                    if label_tag and value_tag:
                        label_text = label_tag.get_text(strip=True).lower()
                        value_text = value_tag.get_text(strip=True)
                        
                        if "price" in label_text:
                            admission_price = value_text
                
                # Extract Event Type
                event_type_tag = soup.find("a", href=re.compile(r"/event-type/"))
                event_type = event_type_tag.text.strip() if event_type_tag else "No Event Type"
                
                # Extract Description
                description_tag = soup.find("div", class_="body")
                description = description_tag.get_text(strip=True, separator=" ") if description_tag else "No Description"
                
                # Extract Image URL
                image_tag = soup.find("img", class_="img-responsive")
                image_url = image_tag["src"] if image_tag and image_tag.has_attr("src") else "No Image"
                
                # Format the data to match our standard structure
                event_data = {
                    "Event_Title": title,
                    "Image_URL": image_url,
                    "Start_Time": start_date,
                    "End_Time": "No End Time",
                    "Location": location,
                    "Full_Address": location,
                    "Categories": event_type,
                    "Admission": admission_price,
                    "Description": description,
                    "Event_URL": URL,
                    "Contact": contact,
                    "Email": email,
                    "Event_Timings": event_timing
                }
                
                event_details_list.append(event_data)
                
            except Exception as event_error:
                logger.error(f"Error processing event at {URL}: {str(event_error)}")
                
            # Wait before moving to the next event
            time.sleep(2)
        
        # Close the driver
        driver.quit()
        
        # If no event details were found, log an error
        if not event_details_list:
            logger.error("No event details found")
            raise Exception("No event details found")
            
        # Convert to DataFrame
        df_events_details = pd.DataFrame(event_details_list)
        
        # Create temp directory for this website
        site_temp_dir = os.path.join(TEMP_DIR, WEBSITE_NAME)
        os.makedirs(site_temp_dir, exist_ok=True)
        
        # Save DataFrame to file
        output_file = os.path.join(site_temp_dir, 'scraped_events.json')
        df_events_details.to_json(output_file, orient='records')
        
        logger.info(f"Successfully scraped {len(df_events_details)} events and saved to {output_file}")
        
        # Pass the file path to the next task via XCom
        context['ti'].xcom_push(key='events_data', value=output_file)
        
        return df_events_details
        
    except Exception as e:
        logger.error(f"Error scraping Boston Gov: {str(e)}")
        # Take a screenshot if WebDriver is still available
        try:
            if 'driver' in locals() and driver:
                screenshot_path = "/tmp/error_screenshot.png"
                driver.save_screenshot(screenshot_path)
                logger.info(f"Error screenshot saved to {screenshot_path}")
                driver.quit()
        except:
            pass
        raise