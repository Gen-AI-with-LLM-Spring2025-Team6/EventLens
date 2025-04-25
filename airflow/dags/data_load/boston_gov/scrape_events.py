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

WEBSITE_NAME = "boston_gov"
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def scrape_boston_gov(**context):
    try:
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument('--disable-software-rasterizer')
        options.add_argument("--disable-extensions")
        options.binary_location = "/usr/bin/chromium"
        service = Service(executable_path="/usr/bin/chromedriver")
        driver = webdriver.Chrome(service=service, options=options)

        BASE_URL = "https://www.boston.gov/events?page="
        event_list = []

        for page in range(3):
            url = BASE_URL + str(page)
            logger.info(f"Scraping page: {page + 1} -> {url}")
            driver.get(url)
            time.sleep(5)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            articles = soup.find_all("article", class_="calendar-listing-wrapper")

            if not articles:
                logger.warning(f"No events found on page {page + 1}")
            else:
                logger.info(f"Found {len(articles)} events on page {page + 1}")

            for article in articles:
                title_tag = article.find("div", class_="title")
                title = title_tag.text.strip() if title_tag else "No Title"

                details_link_tag = article.find("a", class_="button")
                details_link = "https://www.boston.gov" + details_link_tag["href"] if details_link_tag and details_link_tag.has_attr("href") else "No Link"

                event_list.append({"Title": title, "Event Details Link": details_link})

            time.sleep(3)

        driver.quit()

        if not event_list:
            logger.error("No events found on Boston.gov")
            raise Exception("No events found on Boston.gov")

        df_events = pd.DataFrame(event_list)
        driver = webdriver.Chrome(service=service, options=options)
        event_details_list = []

        for index, row in df_events.iterrows():
            URL = row["Event Details Link"]
            if not re.match(r"^https://www\.boston\.gov/node/\d+$", URL):
                logger.info(f"Skipping invalid URL: {URL}")
                continue

            logger.info(f"Scraping: {URL}")
            try:
                driver.get(URL)
                time.sleep(5)
                soup = BeautifulSoup(driver.page_source, "html.parser")

                title_tag = soup.find("h1")
                title = title_tag.text.strip() if title_tag else "No Title"

                start_date_tag = soup.find("div", class_="date-title t--intro")
                start_date = start_date_tag.text.strip() if start_date_tag else "No Start Date"
                end_date = start_date

                # Start/End Time and Occurrences
                start_time = "No Start Time"
                end_time = "No End Time"
                occurrences = "No Occurrences"

                timing_container = soup.find("div", class_="detail-item__content")
                if timing_container:
                    primary_time = timing_container.find("div", class_="sb-d detail-item--secondary")
                    if primary_time:
                        time_text = primary_time.get_text(strip=True)
                        time_match = re.match(r"(\d{1,2}:\d{2}\s*[apAP][mM])\s*[–-]\s*(\d{1,2}:\d{2}\s*[apAP][mM])", time_text)
                        if time_match:
                            start_time = time_match.group(1).strip()
                            end_time = time_match.group(2).strip()

                    recurrence_tag = timing_container.find("div", class_="sb-d detail-item__body--tertiary", role="list")
                    if recurrence_tag:
                        occurrences = recurrence_tag.get_text(strip=True)

                # Full Address
                location = "No Location"
                full_address = "No Address"
                venue_block = soup.find("div", itemprop="address")
                if venue_block:
                    venue_name = venue_block.find("div", itemprop="streetAddress")
                    street = venue_name.get_text(separator=" ", strip=True) if venue_name else ""
                    locality_span = venue_block.find("span", itemprop="addressLocality")
                    locality = locality_span.text.strip() if locality_span else ""
                    region_span = venue_block.find("span", itemprop="addressRegion")
                    region = region_span.get_text(strip=True) if region_span else ""
                    postal_span = venue_block.find("span", itemprop="postalCode")
                    postal_code = postal_span.text.strip() if postal_span else ""
                    full_address = f"{street}, {locality}, {region} {postal_code}".strip(", ")

                # Neighborhood
                neighborhood = "No Location"
                all_detail_sections = soup.find_all("div", class_="detail-item detail-item--secondary")
                for section in all_detail_sections:
                    label = section.find("div", class_="detail-item__left")
                    if label and 'neighborhood' in label.get_text(strip=True).lower():
                        neighborhood_tag = section.find("div", class_="detail-item__body--secondary field-item")
                        if neighborhood_tag:
                            neighborhood = neighborhood_tag.get_text(strip=True)
                        break

                # Contact
                contact_tag = soup.find("div", class_="detail-item__body detail-item__body--secondary field-item")
                contact = contact_tag.text.strip() if contact_tag else "No Contact Info"

                # Email
                email_tag = soup.find("a", href=lambda x: x and x.startswith("mailto:"))
                email = email_tag["href"].replace("mailto:", "") if email_tag else "No Email"

                # Admission Price
                admission_price = "No Price Info"
                details_list = soup.find_all("li", class_="dl-i evt-sb-i")
                for detail in details_list:
                    label_tag = detail.find("div", class_="dl-t")
                    value_tag = detail.find("div", class_="detail-item__body--secondary field-item", role="listitem")
                    if label_tag and value_tag:
                        label_text = label_tag.get_text(strip=True).lower()
                        value_text = value_tag.get_text(strip=True)
                        if "price" in label_text:
                            admission_price = value_text

                # Category
                event_type_tag = soup.find("a", href=re.compile(r"/event-type/"))
                event_type = event_type_tag.text.strip() if event_type_tag else "No Event Type"

                # Description
                description_tag = soup.find("div", class_="body")
                description = description_tag.get_text(strip=True, separator=" ") if description_tag else "No Description"

                # Image URL
                image_tag = soup.find("img", class_="img-responsive")
                image_url = image_tag["src"] if image_tag and image_tag.has_attr("src") else "No Image"

                event_data = {
                    "Event_Title": title,
                    "Image_URL": image_url,
                    "Start_Date": start_date,
                    "End_Date": end_date,
                    "Start_Time": start_time,
                    "End_Time": end_time,
                    "Occurrences": occurrences,
                    "Location": neighborhood,
                    "Full_Address": full_address,
                    "Categories": event_type,
                    "Admission": admission_price,
                    "Description": description,
                    "Event_URL": URL,
                    "Contact": contact,
                    "Email": email
                }

                event_details_list.append(event_data)

            except Exception as event_error:
                logger.error(f"Error processing event at {URL}: {str(event_error)}")

            time.sleep(2)

        driver.quit()

        if not event_details_list:
            logger.error("No event details found")
            raise Exception("No event details found")

        df_events_details = pd.DataFrame(event_details_list)
        print(df_events_details)

        site_temp_dir = os.path.join(TEMP_DIR, WEBSITE_NAME)
        os.makedirs(site_temp_dir, exist_ok=True)
        output_file = os.path.join(site_temp_dir, 'scraped_events.json')
        df_events_details.to_json(output_file, orient='records')

        logger.info(f"Successfully scraped {len(df_events_details)} events and saved to {output_file}")
        context['ti'].xcom_push(key='events_data', value=output_file)
        return df_events_details
    

    except Exception as e:
        logger.error(f"Error scraping Boston Gov: {str(e)}")
        try:
            if 'driver' in locals() and driver:
                screenshot_path = "/tmp/error_screenshot.png"
                driver.save_screenshot(screenshot_path)
                logger.info(f"Error screenshot saved to {screenshot_path}")
                driver.quit()
        except:
            pass
        raise
