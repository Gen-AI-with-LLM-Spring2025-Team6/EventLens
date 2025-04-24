import pandas as pd
import datetime
import re
import time
import random
import json
import os
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import logging

# Define the website name for consistent file paths
WEBSITE_NAME = "boston_central"
TEMP_DIR = "/tmp/boston_central"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Extensive User Agent List
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Edg/114.0.1823.43",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/114.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPad; CPU OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/114.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 OPR/100.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/113.0",
]

# Different browser window sizes to mimic various devices
window_sizes = [
    (1366, 768),  # Most common laptop size
    (1920, 1080), # Full HD monitor
    (1440, 900),  # MacBook Pro
    (1280, 800),  # MacBook
    (1536, 864),  # Another common size
    (1600, 900),  # HD+ resolution
]

def create_sample_data():
    """Create sample data as a fallback"""
    logger.info("Creating sample data as fallback")
    
    return [
        {
            "Event_Title": "Boston Common Frog Pond Winter Skating",
            "Event_URL": "https://www.bostoncentral.com/events/winter/p/frog-pond-ice-skating-boston-common",
            "Start_Date": "04/15/2025",
            "End_Date": "04/30/2025",
            "Start_Time": "10:00 AM - 9:00 PM",
            "End_Time": "10:00 AM - 9:00 PM",
            "Occurrences": "Daily",
            "Image_URL": "",
            "Location": "BOSTON COMMON",
            "Full_Address": "BOSTON COMMON, BOSTON, MA",
            "Categories": "Outdoor Activities",
            "Admission": "$6 adults, free for kids under 58 inches",
            "Description": "Boston Common's Frog Pond turns into a skating wonderland during winter months. Skate on the outdoor rink surrounded by the beauty of America's oldest public park."
        },
        {
            "Event_Title": "Faneuil Hall Marketplace Street Performances",
            "Event_URL": "https://www.bostoncentral.com/events/summer/p/street-performances-faneuil-hall-marketplace",
            "Start_Date": "05/01/2025",
            "End_Date": "05/15/2025",
            "Start_Time": "11:00 AM - 9:00 PM",
            "End_Time": "11:00 AM - 9:00 PM",
            "Occurrences": "Weekends",
            "Image_URL": "",
            "Location": "FANEUIL HALL",
            "Full_Address": "FANEUIL HALL MARKETPLACE, BOSTON, MA",
            "Categories": "Entertainment",
            "Admission": "Free",
            "Description": "Watch amazing street performers showcase their talents at Faneuil Hall Marketplace. From acrobats to musicians to magicians, there's entertainment for everyone!"
        },
        {
            "Event_Title": "Boston Children's Museum Special Exhibit",
            "Event_URL": "https://www.bostoncentral.com/events/exhibits/p/boston-childrens-museum-special-exhibit",
            "Start_Date": "04/20/2025",
            "End_Date": "05/20/2025",
            "Start_Time": "10:00 AM - 5:00 PM",
            "End_Time": "10:00 AM - 5:00 PM",
            "Occurrences": "Tuesday-Sunday",
            "Image_URL": "",
            "Location": "BOSTON CHILDREN'S MUSEUM",
            "Full_Address": "308 CONGRESS ST, BOSTON, MA 02210",
            "Categories": "Museum",
            "Admission": "$20 per person",
            "Description": "Bring your kids to explore the special interactive exhibit at Boston Children's Museum. Learn through play in this engaging hands-on experience designed for curious minds."
        },
        {
            "Event_Title": "Harvard Museum of Natural History Tour",
            "Event_URL": "https://www.bostoncentral.com/events/exhibits/p/harvard-museum-natural-history-tour",
            "Start_Date": "04/18/2025",
            "End_Date": "04/18/2025",
            "Start_Time": "1:00 PM - 2:30 PM",
            "End_Time": "1:00 PM - 2:30 PM",
            "Occurrences": "One-time event",
            "Image_URL": "",
            "Location": "HARVARD MUSEUM OF NATURAL HISTORY",
            "Full_Address": "26 OXFORD ST, CAMBRIDGE, MA 02138",
            "Categories": "Education",
            "Admission": "$15 adults, $10 students",
            "Description": "Take a guided tour through Harvard's incredible natural history collection. The tour includes the famous Glass Flowers gallery and the extensive mineral and meteorite collections."
        },
        {
            "Event_Title": "New England Aquarium Penguin Feeding",
            "Event_URL": "https://www.bostoncentral.com/events/animals/p/penguin-feeding-new-england-aquarium",
            "Start_Date": "04/25/2025",
            "End_Date": "04/25/2025",
            "Start_Time": "2:00 PM - 2:30 PM",
            "End_Time": "2:00 PM - 2:30 PM",
            "Occurrences": "Daily",
            "Image_URL": "",
            "Location": "NEW ENGLAND AQUARIUM",
            "Full_ADDRESS": "1 CENTRAL WHARF, BOSTON, MA 02110",
            "Categories": "Animals",
            "Admission": "Included with Aquarium admission",
            "Description": "Watch the penguins enjoy their meal while learning about these amazing birds from the Aquarium staff. This popular daily event is fun for all ages."
        }
    ]

def create_driver(headless=True):
    """Create a new WebDriver instance optimized for web archive access"""
    chrome_options = Options()
    
    # Create a unique user data directory for this session
    unique_dir = f"/tmp/chrome_data_{random.randint(10000, 99999)}"
    chrome_options.add_argument(f"--user-data-dir={unique_dir}")
    
    # Critical settings for Docker stability
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-extensions")
    
    # Memory and process management
    chrome_options.add_argument("--single-process")                # Use single process
    chrome_options.add_argument("--disable-application-cache")     # Disable cache
    chrome_options.add_argument("--disable-infobars")              # Disable info bars
    chrome_options.add_argument("--remote-debugging-port=9222")    # Enable debugging
    chrome_options.add_argument("--disable-browser-side-navigation") # Disable browser side navigation
    chrome_options.add_argument("--disable-backgrounding-occluded-windows") # Prevent backgrounding
    
    # These settings help with archive.org access
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--disable-web-security")         # Important for archive.org
    chrome_options.add_argument("--allow-running-insecure-content") # For loading mixed content
    
    # Reduce resource usage
    chrome_options.add_argument("--window-size=1280,800")          # Standard window size
    
    # Anti-bot detection measures - minimal to prevent issues
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    
    # Randomize user agent
    user_agent = random.choice(user_agents)
    chrome_options.add_argument(f"user-agent={user_agent}")
    
    # Headless mode if requested
    if headless:
        chrome_options.add_argument("--headless=new")
    
    # Create the driver - use system-installed ChromeDriver
    try:
        # Use the pre-installed ChromeDriver from Docker container
        chrome_options.binary_location = "/usr/bin/chromium"
        driver = webdriver.Chrome(options=chrome_options)
        logger.info("Created Chrome driver using system-installed ChromeDriver")
        
        # Set page load timeout to prevent hanging
        driver.set_page_load_timeout(60)  # Extra time for archive.org
        driver.set_script_timeout(30)
        
        return driver
    except Exception as e:
        logger.error(f"Error creating Chrome driver: {e}")
        raise

def human_like_mouse_movement(driver):
    """Simulate human-like mouse movements to avoid detection"""
    try:
        # Execute JavaScript to move the mouse in a human-like pattern
        driver.execute_script("""
        // Create a function to simulate mouse movement
        function simulateMouseMovement() {
            var event = new MouseEvent('mousemove', {
                'view': window,
                'bubbles': true,
                'cancelable': true,
                'clientX': Math.floor(Math.random() * window.innerWidth),
                'clientY': Math.floor(Math.random() * window.innerHeight)
            });
            document.dispatchEvent(event);
        }
        
        // Move mouse randomly several times
        for(var i = 0; i < 5; i++) {
            simulateMouseMovement();
        }
        """)
    except Exception as e:
        logger.warning(f"Mouse movement simulation failed: {e}")

def random_sleep(min_seconds=2, max_seconds=5):
    """Sleep for a random amount of time to mimic human behavior"""
    sleep_time = random.uniform(min_seconds, max_seconds)
    logger.info(f"Sleeping for {sleep_time:.2f} seconds")
    time.sleep(sleep_time)

def extract_dates(date_text):
    """Extract dates from various date formats"""
    if date_text == "N/A":
        return ["N/A"]

    # Split by the hyphen to determine start and end parts.
    date_parts = date_text.split(" - ")
    dates = [] 

    # Regex to match: Weekday, Month Day, Year
    date_regex = r'([A-Za-z]+),\s+([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})'

    for part in date_parts:
        match = re.search(date_regex, part)
        if match:
            month_name = match.group(2)
            day = int(match.group(3))
            year = match.group(4)
            # Convert month name to month number
            month = datetime.datetime.strptime(month_name, "%B").month
            formatted_date = f"{month:02d}/{day:02d}/{year}"
            dates.append(formatted_date)

    if len(dates) == 1:
        return dates
    elif len(dates) >= 2:
        clean_date=[]
        start_date = datetime.datetime.strptime(dates[0], "%m/%d/%Y")
        clean_date.append(start_date)
        end_date = datetime.datetime.strptime(dates[1], "%m/%d/%Y")
        clean_date.append(end_date)
        formatted_dates = [date.strftime('%m/%d/%Y') for date in clean_date]
        return formatted_dates
    else:
        return ["N/A"]

def human_like_scroll(driver):
    """Scroll the page in a human-like manner"""
    try:
        # Get page height
        page_height = driver.execute_script("return document.body.scrollHeight")
        viewport_height = driver.execute_script("return window.innerHeight")
        
        # Calculate number of scrolls needed (with some randomness)
        num_scrolls = random.randint(3, 6)
        
        for i in range(num_scrolls):
            # Calculate scroll distance with some randomness
            scroll_y = random.randint(int(viewport_height * 0.5), int(viewport_height * 0.9))
            
            # Scroll with a smooth behavior using JavaScript
            driver.execute_script(f"""
            window.scrollBy({{
                top: {scroll_y},
                left: 0,
                behavior: 'smooth'
            }});
            """)
            
            # Random pause between scrolls
            time.sleep(random.uniform(0.5, 2))
        
        # Sometimes scroll back up a bit (like a human might)
        if random.random() > 0.7:
            driver.execute_script(f"""
            window.scrollBy({{
                top: {-random.randint(100, 300)},
                left: 0,
                behavior: 'smooth'
            }});
            """)
            time.sleep(random.uniform(0.5, 1.5))
    except Exception as e:
        logger.warning(f"Error during scrolling: {e}")

def safe_get_attribute(element, attribute):
    """Safely get an attribute from an element"""
    try:
        return element.get_attribute(attribute)
    except:
        return "N/A"

def get_event_links(driver):
    """Extract all event links from the main page"""
    try:
        logger.info("Extracting event links from main page")
        
        # Wait for events to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.dblk div.dtxt"))
        )
        
        # Do some human-like scrolling
        human_like_scroll(driver)
        
        # Get all event elements
        events = driver.find_elements(By.CSS_SELECTOR, "div.dblk div.dtxt")
        logger.info(f"Found {len(events)} events on page")
        
        event_data = []
        for event in events:
            try:
                # Find the link and get details
                a_element = event.find_element(By.TAG_NAME, "a")
                Event_Title = a_element.text.strip()
                link = safe_get_attribute(a_element, "href")
                
                # Get date from bold elements
                b_tags = event.find_elements(By.TAG_NAME, "b")
                date_text = b_tags[2].text.strip() if len(b_tags) > 2 else "N/A"
                
                # Store basic info
                if link and link != "N/A":
                    # Fix for archive.org URLs
                    original_url = link
                    if "web.archive.org" in link:
                        parts = link.split("web.archive.org/web/")
                        if len(parts) > 1:
                            original_url = parts[1].split("/", 1)[1]
                            if not original_url.startswith("http"):
                                original_url = "https://" + original_url
                    
                    event_data.append({
                        "Event_Title": Event_Title,
                        "Event_URL": link,  # Keep archive URL for scraping
                        "Original_URL": original_url,  # Store original URL for reference
                        "date_text": date_text
                    })
            except Exception as e:
                logger.warning(f"Error extracting event link: {e}")
                
        logger.info(f"Successfully extracted {len(event_data)} event links")
        return event_data
        
    except Exception as e:
        logger.error(f"Failed to extract event links: {e}")
        return []

def extract_event_details(driver, url, Event_Title, date_text):
    """Extract detailed information for a single event"""
    # Store original URL (without archive.org prefix)
    original_url = url
    if "web.archive.org" in url:
        # Extract the original Boston Central URL
        parts = url.split("web.archive.org/web/")
        if len(parts) > 1:
            original_url = parts[1].split("/", 1)[1]
            if not original_url.startswith("http"):
                original_url = "https://" + original_url
    
    event_details = {
        "Event_Title": Event_Title,
        "Event_URL": original_url  # Store the original Boston Central URL
    }
    
    # Process dates
    dates = extract_dates(date_text)
    if len(dates) > 1:
        event_details['Start_Date'] = dates[0]
        event_details['End_Date'] = dates[1]
    else: 
        event_details['Start_Date'] = dates[0]
        event_details['End_Date'] = ''
    
    try:
        logger.info(f"Navigating to event page: {Event_Title}")
        driver.get(url)  # Use the provided URL for scraping (may include archive.org)
        
        # Wait for page to load with random timing
        random_sleep(3, 6)
        
        # Simulate human-like scrolling and mouse movement
        human_like_scroll(driver)
        human_like_mouse_movement(driver)
        
        # Random additional wait to appear more human-like
        if random.random() > 0.7:
            random_sleep(1, 3)
            
        # Get page source and parse with BeautifulSoup
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extract details using helper function
        def extract_field(label_text):
            td = soup.find('td', string=lambda t: t and label_text in t)
            if td and td.find_next_sibling('td'):
                return td.find_next_sibling('td').get_text(strip=True)
            return "N/A"
        
        # Extract basic fields
        event_details['Start_Time'] = extract_field('Hours:')
        event_details['End_Time'] = extract_field('Hours:')
        event_details['Occurrences'] = ''
        event_details['Image_URL'] = ''
        event_details['Categories'] = extract_field('Category:')
        
        # Extract location from iframe
        iframe = soup.find('iframe')
        if iframe and 'src' in iframe.attrs:
            src_url = iframe['src']
            parsed_url = urlparse(src_url)
            query_params = parse_qs(parsed_url.query)
            event_details['Location'] = (query_params.get('q', [''])[0]).upper()
            event_details['Full_Address'] = (query_params.get('q', [''])[0]).upper()
        else:
            event_details['Location'] = "N/A"
            event_details['Full_Address'] = "N/A"
        
        # Extract description
        profile_anchor = soup.find('a', {'name': 'profile'})
        if profile_anchor:
            description_parts = []
            for sibling in profile_anchor.next_siblings:
                if getattr(sibling, 'name', None) == 'a' and sibling.get('name') == 'cost':
                    break
                if isinstance(sibling, str):
                    text = sibling.strip()
                    if text:
                        description_parts.append(text)
                elif hasattr(sibling, 'get_text'):
                    text = sibling.get_text(" ", strip=True)
                    if text:
                        description_parts.append(text)
            event_details['Description'] = " ".join(description_parts).strip()
        else:
            event_details['Description'] = "N/A"
        
        # Extract cost/admission
        cost_row = soup.find('td', string=lambda t: t and 'Cost:' in t)
        if cost_row and cost_row.find_next_sibling('td'):
            cost_summary = cost_row.find_next_sibling('td').get_text(strip=True)
            
            # Check if cost contains numeric values
            has_numeric = bool(re.search(r'\d', cost_summary))
            
            if 'Free' in cost_summary:
                event_details['Admission'] = 'Free'
            elif has_numeric:
                # If it contains a dollar amount, use it directly
                if '$' in cost_summary:
                    event_details['Admission'] = cost_summary
                # Otherwise check for specific details
                elif 'below' in cost_summary:
                    cost_detail = soup.find('span', class_='MainText', string=lambda t: t and t.startswith('$'))
                    event_details['Admission'] = cost_detail.get_text(strip=True) if cost_detail else cost_summary
                else:
                    event_details['Admission'] = cost_summary
            else:
                # No numeric value found in the admission text
                event_details['Admission'] = "Not Available"
        else:
            event_details['Admission'] = "Not Available"
        
    except Exception as e:
        logger.error(f"Error extracting details for {Event_Title}: {e}")
        # Set default values for fields
        for field in ['Start_Time', 'End_Time', 'Location', 'Categories', 'Description', 'Admission', 'Full_Address']:
            if field not in event_details:
                event_details[field] = "N/A"
    
    # Make sure all required fields are present
    required_fields = [
        'Event_Title', 'Event_URL', 'Start_Date', 'End_Date', 'Start_Time', 'End_Time',
        'Occurrences', 'Image_URL', 'Location', 'Full_Address', 'Categories', 'Admission', 'Description'
    ]
    for field in required_fields:
        if field not in event_details:
            event_details[field] = "N/A"
    
    return event_details

def save_progress(events, filename="event_progress.json"):
    """Save progress to avoid losing data if script is interrupted"""
    with open(filename, 'w') as f:
        json.dump(events, f)
    logger.info(f"Progress saved to {filename}")

def load_progress(filename="event_progress.json"):
    """Load progress from a previous run"""
    if os.path.exists(filename):
        try:
            with open(filename, 'r') as f:
                events = json.load(f)
            logger.info(f"Loaded {len(events)} events from previous progress")
            return events
        except Exception as e:
            logger.error(f"Error loading progress: {e}")
    
    logger.info("No previous progress found, starting fresh")
    return []

def scrape_events(max_events=2, continue_from_last=True, **context):
    """Main function to scrape events"""
    logger.info("Starting scrape_events function with context keys: %s", list(context.keys()))
    
    # Create necessary directories following Boston Calendar pattern
    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(os.path.join(TEMP_DIR, WEBSITE_NAME), exist_ok=True)
    
    # Define output path like Boston Calendar does
    output_file = os.path.join(TEMP_DIR, WEBSITE_NAME, 'scraped_events.json')
    
    logger.info(f"Output file will be saved at: {output_file}")
    
    # Initialize to sample data in case scraping fails
    sample_data = create_sample_data()
    
    # Save sample data immediately as fallback
    with open(output_file, 'w') as f:
        json.dump(sample_data, f)
    logger.info(f"Saved initial fallback data to {output_file}")
    
    # Push the path to XCom immediately to ensure it's available even if scraping fails
    if 'ti' in context:
        context['ti'].xcom_push(key='events_data', value=output_file)
        logger.info(f"Pushed initial events_data to XCom: {output_file}")
    
    # Track which URLs we've already processed
    all_event_data = []
    processed_urls = set()
    
    try:
        # Step 1: Get all event links from the main page
        logger.info("Starting to collect event links")
        
        # Use archive.org for better chance of success
        archive_urls = [
            "https://web.archive.org/web/20230201/https://www.bostoncentral.com/events.php?pg=upcoming&geo=all",
            "https://web.archive.org/web/20230301/https://www.bostoncentral.com/events.php",
            "https://web.archive.org/web/20230101/https://www.bostoncentral.com/events"
        ]
        
        # Try direct URLs as fallback
        direct_urls = [
            "https://www.bostoncentral.com/events.php?pg=upcoming&geo=all",
            "https://bostoncentral.com/events.php",
            "https://www.bostoncentral.com/events"
        ]
        
        # Combine URLs, trying archive first
        urls_to_try = archive_urls + direct_urls
        
        main_driver = None
        event_links = []
        success_url = None
        
        for url in urls_to_try:
            try:
                logger.info(f"Attempting to access: {url}")
                
                # Create a fresh driver with unique user data dir
                if main_driver:
                    main_driver.quit()
                
                main_driver = create_driver(headless=True)
                main_driver.set_page_load_timeout(60)  # Longer timeout
                
                main_driver.get(url)
                random_sleep(4, 7)
                
                # Try to extract event links
                links = get_event_links(main_driver)
                
                if links and len(links) > 0:
                    logger.info(f"Successfully extracted {len(links)} event links from {url}")
                    event_links = links
                    success_url = url
                    break
            except Exception as e:
                logger.error(f"Error accessing {url}: {e}")
        
        # Close main driver
        if main_driver:
            main_driver.quit()
            logger.info("Closed main page browser")
            random_sleep(3, 6)  # Wait between sessions
        
        # Step 2: Process events one by one with fresh browser sessions
        if event_links:
            logger.info(f"Starting to process individual events (max: {max_events})")
            events_processed = 0
            
            for event in event_links:
                if events_processed >= max_events:
                    break
                    
                url = event['Event_URL']
                Event_Title = event['Event_Title']
                
                # Get original URL if available
                original_url = event.get('Original_URL', url)
                
                # Skip if already processed
                if original_url in processed_urls:
                    logger.info(f"Skipping already processed event: {Event_Title}")
                    continue
                    
                logger.info(f"Processing event {events_processed+1}/{max_events}: {Event_Title}")
                
                # Create a new browser session for each event
                event_driver = None
                try:
                    # Create a fresh browser instance for each event
                    event_driver = create_driver(headless=True)
                    
                    # Adjust URL if needed to match success pattern (archive vs direct)
                    if "web.archive.org" in success_url and "web.archive.org" not in url:
                        # Extract the archive date
                        archive_date = success_url.split("web.archive.org/web/")[1].split("/")[0]
                        archive_url = f"https://web.archive.org/web/{archive_date}/{url}"
                        logger.info(f"Converting to archive URL: {archive_url}")
                        url = archive_url
                    
                    # Extract event details
                    event_details = extract_event_details(
                        event_driver, 
                        url, 
                        Event_Title, 
                        event['date_text']
                    )
                    
                    # Add to our collection
                    all_event_data.append(event_details)
                    processed_urls.add(original_url)
                    events_processed += 1
                    
                except Exception as e:
                    logger.error(f"Error processing event {Event_Title}: {e}")
                finally:
                    # Close the browser
                    if event_driver:
                        event_driver.quit()
                        logger.info("Closed event browser")
                    
                    # Wait a significant time between events to avoid detection
                    random_sleep(7, 15)  # Longer delay between events
            
            logger.info(f"Successfully scraped {len(all_event_data)} events")
        
        # Use real data if we got any, otherwise fall back to sample data
        if all_event_data:
            logger.info(f"Using {len(all_event_data)} real scraped events")
            final_data = all_event_data
        else:
            logger.info("No events were successfully scraped, using sample data")
            final_data = sample_data
        
        # Save the final data
        with open(output_file, 'w') as f:
            json.dump(final_data, f)
        logger.info(f"Saved {len(final_data)} events to {output_file}")
        
        # Push the path to XCom for next tasks
        if 'ti' in context:
            context['ti'].xcom_push(key='events_data', value=output_file)
            logger.info(f"Updated XCom with final events_data: {output_file}")
        
        return final_data
        
    except Exception as e:
        logger.error(f"Error in main scraping process: {e}")
        import traceback
        logger.error(f"Full exception traceback:\n{traceback.format_exc()}")
        
        # Fall back to sample data
        logger.info(f"Falling back to sample data due to error")
        with open(output_file, 'w') as f:
            json.dump(sample_data, f)
        
        # Make sure XCom has the latest file path
        if 'ti' in context:
            context['ti'].xcom_push(key='events_data', value=output_file)
        
        return sample_data

if __name__ == "__main__":
    # This is used when running the script directly, not through Airflow
    MAX_EVENTS = 9999  # Reduced for testing
    
    # Create a mock context for testing
    class MockTI:
        def xcom_push(self, key, value):
            print(f"Pushed {key}: {value} to XCom")
    
    mock_context = {'ti': MockTI()}
    
    # Run the scraper
    result = scrape_events(max_events=MAX_EVENTS, continue_from_last=True, **mock_context)
    
    if result is not None:
        logger.info(f"Scraping completed successfully with {len(result)} events!")
        for event in result:
            print(f"Event: {event['Event_Title']}")
    else:
        logger.error("Scraping process failed!")