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

def create_driver(headless=False):
    """Create a new WebDriver instance with advanced anti-detection features"""
    chrome_options = Options()
    
    # Basic settings
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    # Anti-bot detection measures
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    chrome_options.add_argument("--disable-extensions")
    
    # Additional anti-detection
    chrome_options.add_argument("--disable-features=IsolateOrigins,site-per-process")
    chrome_options.add_argument("--disable-site-isolation-trials")
    chrome_options.add_argument("--disable-web-security")
    
    # Randomize window size
    width, height = random.choice(window_sizes)
    chrome_options.add_argument(f"--window-size={width},{height}")
    
    # Randomize user agent
    user_agent = random.choice(user_agents)
    chrome_options.add_argument(f"user-agent={user_agent}")
    
    # Headless mode if requested
    if headless:
        chrome_options.add_argument("--headless=new")
    
    # Create the driver
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    # Execute CDP commands to modify properties to avoid detection
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
        // Overwrite the 'navigator.webdriver' property
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
        
        // Modify navigator properties
        const newProto = navigator.__proto__;
        delete newProto.webdriver;
        navigator.__proto__ = newProto;
        
        // Add fake plugins
        Object.defineProperty(navigator, 'plugins', {
            get: () => {
                return [
                    {
                        0: {type: "application/x-google-chrome-pdf"},
                        description: "Portable Document Format",
                        filename: "internal-pdf-viewer",
                        name: "Chrome PDF Plugin"
                    },
                    {
                        0: {type: "application/pdf"},
                        description: "Portable Document Format",
                        filename: "internal-pdf-viewer",
                        name: "Chrome PDF Viewer"
                    },
                    {
                        0: {type: "application/x-nacl"},
                        description: "Native Client Executable",
                        filename: "internal-nacl-plugin",
                        name: "Native Client"
                    }
                ];
            }
        });
        
        // Add languages
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-US', 'en', 'es', 'fr', 'de']
        });
        
        // Override permissions API
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );
        
        // Prevent detection through Chrome DevTools Protocol
        window.chrome = {
            app: {
                isInstalled: false,
                InstallState: {
                    DISABLED: 'disabled',
                    INSTALLED: 'installed',
                    NOT_INSTALLED: 'not_installed'
                },
                RunningState: {
                    CANNOT_RUN: 'cannot_run',
                    READY_TO_RUN: 'ready_to_run',
                    RUNNING: 'running'
                }
            },
            runtime: {
                OnInstalledReason: {
                    CHROME_UPDATE: 'chrome_update',
                    INSTALL: 'install',
                    SHARED_MODULE_UPDATE: 'shared_module_update',
                    UPDATE: 'update'
                },
                OnRestartRequiredReason: {
                    APP_UPDATE: 'app_update',
                    OS_UPDATE: 'os_update',
                    PERIODIC: 'periodic'
                },
                PlatformArch: {
                    ARM: 'arm',
                    ARM64: 'arm64',
                    MIPS: 'mips',
                    MIPS64: 'mips64',
                    X86_32: 'x86-32',
                    X86_64: 'x86-64'
                },
                PlatformNaclArch: {
                    ARM: 'arm',
                    MIPS: 'mips',
                    MIPS64: 'mips64',
                    X86_32: 'x86-32',
                    X86_64: 'x86-64'
                },
                PlatformOs: {
                    ANDROID: 'android',
                    CROS: 'cros',
                    LINUX: 'linux',
                    MAC: 'mac',
                    OPENBSD: 'openbsd',
                    WIN: 'win'
                },
                RequestUpdateCheckStatus: {
                    NO_UPDATE: 'no_update',
                    THROTTLED: 'throttled',
                    UPDATE_AVAILABLE: 'update_available'
                }
            }
        };
        
        // Override WebGL vendor and renderer
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(parameter) {
            // UNMASKED_VENDOR_WEBGL
            if (parameter === 37445) {
                return 'Intel Inc.';
            }
            // UNMASKED_RENDERER_WEBGL
            if (parameter === 37446) {
                return 'Intel Iris OpenGL Engine';
            }
            return getParameter.apply(this, arguments);
        };
        """
    })
    
    return driver

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
        random_sleep(0.5, 2)
    
    # Sometimes scroll back up a bit (like a human might)
    if random.random() > 0.7:
        driver.execute_script(f"""
        window.scrollBy({{
            top: {-random.randint(100, 300)},
            left: 0,
            behavior: 'smooth'
        }});
        """)
        random_sleep(0.5, 1.5)

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
                title = a_element.text.strip()
                link = safe_get_attribute(a_element, "href")
                
                # Get date from bold elements
                b_tags = event.find_elements(By.TAG_NAME, "b")
                date_text = b_tags[2].text.strip() if len(b_tags) > 2 else "N/A"
                
                # Store basic info
                if link and link != "N/A":
                    event_data.append({
                        "title": title,
                        "link": link,
                        "date_text": date_text
                    })
            except Exception as e:
                logger.warning(f"Error extracting event link: {e}")
                
        logger.info(f"Successfully extracted {len(event_data)} event links")
        return event_data
        
    except Exception as e:
        logger.error(f"Failed to extract event links: {e}")
        return []

def extract_event_details(driver, url, title, date_text):
    """Extract detailed information for a single event"""
    event_details = {
        "title": title,
        "link": url
    }
    
    # Process dates
    dates = extract_dates(date_text)
    if len(dates) > 1:
        event_details['start_date'] = dates[0]
        event_details['end_date'] = dates[1]
    else: 
        event_details['start_date'] = dates[0]
        event_details['end_date'] = ''
    
    try:
        logger.info(f"Navigating to event page: {title}")
        driver.get(url)
        
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
        event_details['hours'] = extract_field('Hours:')
        event_details['ages'] = extract_field('Ages:')
        event_details['Indoor/Outdoor'] = extract_field('In/Outdoor:')
        event_details['Category'] = extract_field('Category:')
        
        # Extract website
        website_element = soup.find('a', attrs={'name': 'website'})
        if website_element:
            next_p = website_element.find_next('p')
            if next_p and next_p.find('a'):
                event_details['website'] = next_p.find('a')['href']
            else:
                event_details['website'] = "N/A"
        else:
            event_details['website'] = "N/A"
        
        # Extract location from iframe
        iframe = soup.find('iframe')
        if iframe and 'src' in iframe.attrs:
            src_url = iframe['src']
            parsed_url = urlparse(src_url)
            query_params = parse_qs(parsed_url.query)
            event_details['location'] = (query_params.get('q', [''])[0]).upper()
        else:
            event_details['location'] = "N/A"
        
        # Extract phone
        phone_text = soup.find(text=re.compile(r'Phone:\s*\d+'))
        if phone_text:
            match = re.search(r'Phone:\s*(\d+)', phone_text)
            event_details['phone'] = match.group(1) if match else 'N/A'
        else:
            event_details['phone'] = 'N/A'
        
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
            event_details['description'] = " ".join(description_parts).strip()
        else:
            event_details['description'] = "N/A"
        
        # Extract cost
        cost_row = soup.find('td', string=lambda t: t and 'Cost:' in t)
        if cost_row and cost_row.find_next_sibling('td'):
            cost_summary = cost_row.find_next_sibling('td').get_text(strip=True)
            if 'Free' in cost_summary:
                event_details['cost'] = 'Free'
            elif 'below' in cost_summary:
                cost_detail = soup.find('span', class_='MainText', string=lambda t: t and t.startswith('$'))
                event_details['cost'] = cost_detail.get_text(strip=True) if cost_detail else cost_summary
            else:
                event_details['cost'] = cost_summary
        else:
            event_details['cost'] = 'N/A'
            
        logger.info(f"Successfully extracted details for: {title}")
        
    except Exception as e:
        logger.error(f"Error extracting details for {title}: {e}")
        # Set default values for fields
        for field in ['hours', 'website', 'location', 'ages', 'Indoor/Outdoor', 
                      'Category', 'phone', 'description', 'cost']:
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

def scrape_events(max_events=5, continue_from_last=True):
    """Main function to scrape events"""
    # Load any existing progress
    all_event_data = load_progress() if continue_from_last else []
    
    # Track which URLs we've already processed
    processed_urls = {event['link'] for event in all_event_data if 'link' in event}
    
    try:
        # Step 1: Get all event links from the main page
        logger.info("Starting to collect event links")
        main_driver = create_driver()
        
        try:
            main_driver.get("https://www.bostoncentral.com/events.php?pg=upcoming&geo=all")
            random_sleep(4, 7)
            event_links = get_event_links(main_driver)
        finally:
            main_driver.quit()
            logger.info("Closed main page browser")
            random_sleep(3, 6)  # Wait between sessions
        
        # Step 2: Process events one by one with fresh browser sessions
        logger.info(f"Starting to process individual events (max: {max_events})")
        events_processed = 0
        
        for event in event_links:
            if events_processed >= max_events:
                break
                
            url = event['link']
            title = event['title']
            
            # Skip if already processed
            if url in processed_urls:
                logger.info(f"Skipping already processed event: {title}")
                continue
                
            logger.info(f"Processing event {events_processed+1}/{max_events}: {title}")
            
            # Create a new browser session for each event
            event_driver = None
            try:
                # Create a fresh browser instance for each event
                event_driver = create_driver()
                
                # Extract event details
                event_details = extract_event_details(
                    event_driver, 
                    url, 
                    title, 
                    event['date_text']
                )
                
                # Add to our collection
                all_event_data.append(event_details)
                processed_urls.add(url)
                events_processed += 1
                
                # Save progress after each event
                save_progress(all_event_data)
                
            except Exception as e:
                logger.error(f"Error processing event {title}: {e}")
            finally:
                # Close the browser
                if event_driver:
                    event_driver.quit()
                    logger.info("Closed event browser")
                
                # Wait a significant time between events to avoid detection
                random_sleep(7, 15)  # Longer delay between events
        
        # Create the final DataFrame
        df = pd.DataFrame(all_event_data)
        df.to_csv('boston_events.csv', index=False)
        logger.info(f"Completed scraping {len(df)} events, saved to boston_events.csv")
        
        return df
        
    except Exception as e:
        logger.error(f"Error in main scraping process: {e}")
        # Still try to save what we have
        if all_event_data:
            df = pd.DataFrame(all_event_data)
            df.to_csv('files/boston_events_partial.csv', index=False)
            logger.info(f"Saved partial results ({len(df)} events) to boston_events_partial.csv")
        return None

if __name__ == "__main__":
    # Set the maximum number of events to scrape
    MAX_EVENTS = 9999
    
    # Run the scraper
    result = scrape_events(max_events=MAX_EVENTS, continue_from_last=True)
    
    if result is not None:
        logger.info("Scraping completed successfully!")
    else:
        logger.error("Scraping process failed!")
