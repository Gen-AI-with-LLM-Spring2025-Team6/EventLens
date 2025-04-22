def extract_events(**context):
    import os
    import time
    import json
    import pandas as pd
    import re
    import requests
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from bs4 import BeautifulSoup
    from airflow.models import Variable
    import logging

    logger = logging.getLogger(__name__)
    
    TEMP_DIR = Variable.get("TEMP_DIR", "/tmp/airflow")
    WEBSITE_NAME = "meetboston"

    # Setup Chrome with local binary and chromedriver
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--window-size=1920,1080")  
    driver = webdriver.Chrome(options=chrome_options)

    base_url = "https://www.meetboston.com/events/?skip="
    base_url2 = "https://www.meetboston.com"
    max_pages = 21
    event_links = []

    for skip in range(0, max_pages * 12, 12):
        url = base_url + str(skip)
        driver.get(url)
        time.sleep(10)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        events = soup.find_all("div", {"data-type": "events"})
        for event in events:
            title_tag = event.find("a", class_="title truncate")
            if title_tag:
                link = title_tag["href"]
                if link.startswith("/"):
                    link = base_url2 + link
                event_links.append(link)
    driver.quit()

    def extract_event_details(event_url):
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            response = requests.get(event_url, headers=headers, timeout=30)
            soup = BeautifulSoup(response.text, "html.parser")

            title_tag = soup.find("h1") or soup.find("title")
            description_tag = soup.find("meta", {"name": "description"})
            script_text = next((s.text for s in soup.find_all("script") if "var cityStateZip" in s.text), "")

            def safe_extract(pattern):
                match = re.search(pattern, script_text)
                return match.group(1) if match else "N/A"

            return {
                "Title": title_tag.text.strip() if title_tag else "N/A",
                "Description": description_tag["content"] if description_tag else "N/A",
                "Full Address": f"{safe_extract(r'var streetAddress = \"(.*?)\";')}, {safe_extract(r'var cityStateZip = \"(.*?)\";')}" if script_text else "N/A",
                "Event Dates": safe_extract(r'var dates = \"(.*?)\";'),
                "Image URL": safe_extract(r'"mediaurl":"(.*?)"'),
                "Buy Tickets Link": soup.find("link", {"rel": "canonical"})["href"] if soup.find("link", {"rel": "canonical"}) else "N/A",
                "Latitude": safe_extract(r'"latitude":(.*?),'),
                "Longitude": safe_extract(r'"longitude":(.*?),'),
                "Link": event_url
            }

        except Exception as e:
            print(f"Error processing {event_url}: {e}")
            return {
                "Title": "N/A", "Description": "N/A", "Full Address": "N/A",
                "Event Dates": "N/A", "Image URL": "N/A", "Buy Tickets Link": "N/A",
                "Latitude": "N/A", "Longitude": "N/A", "Link": event_url
            }

    all_details = [extract_event_details(url) for url in event_links]
    df = pd.DataFrame(all_details)

    # Save to file
    site_temp_dir = os.path.join(TEMP_DIR, WEBSITE_NAME)
    os.makedirs(site_temp_dir, exist_ok=True)
    output_file = os.path.join(site_temp_dir, 'extracted_events.json')
    df.to_json(output_file, orient='records')

    logger.info(f"Extracted {len(df)} events and saved to {output_file}")

    # Push the file path to XCom
    context['ti'].xcom_push(key="extracted_events", value=output_file)

    return df
