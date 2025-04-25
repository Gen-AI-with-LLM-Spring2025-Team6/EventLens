def extract_events(**context):
    # Import necessary libraries for the extraction process
    import os  # Operating system library for file and directory operations
    import time  # Time library for adding delays
    import json  # JSON library to handle JSON files
    import pandas as pd  # Pandas library for data manipulation
    import re  # Regular expressions for pattern matching
    import requests  # Requests library to handle HTTP requests
    from selenium import webdriver  # Selenium for web automation and scraping
    from selenium.webdriver.chrome.options import Options  # Chrome options for configuring the browser
    from bs4 import BeautifulSoup  # BeautifulSoup for parsing HTML
    from airflow.models import Variable  # Airflow for task execution context
    import logging  # Logging for tracking events and errors
    from data_load.parameters.parameter_config import (
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE, 
    SNOWFLAKE_ROLE, TEMP_DIR
)
    # Initialize logger for tracking the process
    logger = logging.getLogger(__name__)  # Create a logger object for the script
    logging.basicConfig(level=logging.INFO)  # Set up logging level to INFO

    WEBSITE_NAME = "meetboston"  # Define the website name for the extraction

    # Setup Chrome options for headless browsing, which means it runs without opening the browser window
    chrome_options = Options()  # Create options object for Chrome
    chrome_options.add_argument("--headless")  # Run Chrome in headless mode (without GUI)
    chrome_options.add_argument("--no-sandbox")  # Disable sandbox for security reasons in some environments
    chrome_options.add_argument("--disable-dev-shm-usage")  # Disable /dev/shm usage to avoid issues in Docker environments
    chrome_options.add_argument("--disable-gpu")  # Disable GPU hardware acceleration
    chrome_options.add_argument("--disable-software-rasterizer")  # Disable software rasterizer
    chrome_options.add_argument("--window-size=1920,1080")  # Set browser window size to 1920x1080
    driver = webdriver.Chrome(options=chrome_options)  # Initialize Chrome WebDriver with the options

    # Define the base URL and other necessary URLs for scraping
    base_url = "https://www.meetboston.com/events/?skip="  # URL format for paginated event listings
    base_url2 = "https://www.meetboston.com"  # Base URL to handle relative links
    max_pages = 21  # Number of pages to scrape
    event_links = []  # List to store the event URLs

    # Loop through the pages to collect event links
    for skip in range(0, max_pages * 12, 12):
        # Construct the URL for each page by adjusting the 'skip' parameter
        url = base_url + str(skip)
        driver.get(url)  # Fetch the page using Selenium
        time.sleep(10)  # Wait for 10 seconds for the page to load completely
        soup = BeautifulSoup(driver.page_source, "html.parser")  # Parse the page source with BeautifulSoup
        events = soup.find_all("div", {"data-type": "events"})  # Find all event elements in the page
        
        # Loop through each event and extract the event links
        for event in events:
            title_tag = event.find("a", class_="title truncate")  # Find the event title link
            if title_tag:
                link = title_tag["href"]  # Extract the href attribute (event URL)
                if link.startswith("/"):  # If the link is relative, make it absolute
                    link = base_url2 + link
                event_links.append(link)  # Add the event link to the list

    driver.quit()  # Quit the WebDriver after scraping is complete

    # Function to extract detailed event information from an individual event URL
    def extract_event_details(event_url):
        try:
            headers = {"User-Agent": "Mozilla/5.0"}  # Set User-Agent header to avoid blocking by the website
            response = requests.get(event_url, headers=headers, timeout=30)  # Send an HTTP GET request to the event page
            soup = BeautifulSoup(response.text, "html.parser")  # Parse the page content with BeautifulSoup

            # Extract various event details using BeautifulSoup and regular expressions
            title_tag = soup.find("h1") or soup.find("title")  # Find event title
            description_tag = soup.find("meta", {"name": "description"})  # Extract event description
            script_text = next((s.text for s in soup.find_all("script") if "var cityStateZip" in s.text), "")  # Extract script data for address

            # Helper function to safely extract data using regular expressions
            def safe_extract(pattern):
                match = re.search(pattern, script_text)  # Apply regex to the script text
                return match.group(1) if match else "N/A"  # Return matched data or "N/A" if no match is found

            # Return a dictionary with extracted event details
            return {
                "TITLE": title_tag.text.strip() if title_tag else "N/A",  # Event title
                "DESCRIPTION": description_tag["content"] if description_tag else "N/A",  # Event description
                "FULL_ADDRESS": f"{safe_extract(r'var streetAddress = \"(.*?)\";')}, {safe_extract(r'var cityStateZip = \"(.*?)\";')}" if script_text else "N/A",  # Full address
                "EVENT_DATES": safe_extract(r'var dates = \"(.*?)\";'),  # Event dates
                "IMAGE_URL": safe_extract(r'"mediaurl":"(.*?)"'),  # Event image URL
                "BUY_TICKETS_LINK": soup.find("link", {"rel": "canonical"})["href"] if soup.find("link", {"rel": "canonical"}) else "N/A",  # Buy tickets link
                "LATITUDE": safe_extract(r'"latitude":(.*?),'),  # Latitude of the event
                "LONGITUDE": safe_extract(r'"longitude":(.*?),'),  # Longitude of the event
                "LINK": event_url  # Event URL
            }

        except Exception as e:
            # In case of an error, log and return a default "N/A" value for all fields
            print(f"Error processing {event_url}: {e}")
            return {
                "TITLE": "N/A", "DESCRIPTION": "N/A", "FULL_ADDRESS": "N/A",
                "EVENT_DATES": "N/A", "IMAGE_URL": "N/A", "BUY_TICKETS_LINK": "N/A",
                "LATITUDE": "N/A", "LONGITUDE": "N/A", "LINK": event_url
            }

    # Loop through all event links and extract details for each event
    all_details = [extract_event_details(url) for url in event_links]
    df = pd.DataFrame(all_details)  # Convert the extracted details into a Pandas DataFrame

    # Print the DataFrame to the console for debugging purposes
    print(df)

    # Define the temporary directory for saving the extracted data
    site_temp_dir = os.path.join(TEMP_DIR, WEBSITE_NAME)
    os.makedirs(site_temp_dir, exist_ok=True)  # Create the directory if it does not exist
    output_file = os.path.join(site_temp_dir, 'extracted_events.json')  # Define the path for the output JSON file

    # Save the extracted event data to a JSON file
    df.to_json(output_file, orient='records')  # Save DataFrame as a JSON file

    # Log the number of events extracted and the file path
    logger.info(f"Extracted {len(df)} events and saved to {output_file}")

    # Push the file path to XCom so the next task can access it
    context['ti'].xcom_push(key="extracted_events", value=output_file)

    # Return the DataFrame of extracted event details
    return df
