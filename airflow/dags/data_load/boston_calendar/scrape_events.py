import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import logging
from datetime import datetime
from dateutil import parser as date_parser

# Replace with your actual TEMP_DIR if needed
TEMP_DIR = "/tmp/boston_calendar"
WEBSITE_NAME = "boston_calendar"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def format_datetime_parts(dt):
    try:
        return dt.strftime("%B %d, %Y"), dt.strftime("%I:%M%p").lower().replace("am", "AM").replace("pm", "PM")
    except:
        return "No Date", "No Time"


def scrape_boston_calendar(**context):
    try:
        today = datetime.today()
        day, month, year = today.day, today.month, today.year

        URL = f"https://www.thebostoncalendar.com/events?day={day}&month={month}&week=1&year={year}"

        logger.info(f"Scraping events from {URL}")

        HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
        }

        response = requests.get(URL, headers=HEADERS)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "lxml")
        events = soup.find_all("li", class_="event")

        event_data = []
        for event in events:
            title_tag = event.find("h3").find("a")
            title = title_tag.get_text(strip=True) if title_tag else "No Title"
            link = title_tag["href"] if title_tag and title_tag.has_attr("href") else "No Link"
            full_link = f"https://www.thebostoncalendar.com{link}" if link.startswith("/") else link
            event_data.append({"Title": title, "Link": full_link})

        df = pd.DataFrame(event_data)
        event_list = []

        for _, row in df.iterrows():
            
            EVENT_URL = row["Link"]
            logger.info(f"Scraping event detail page: {EVENT_URL}")
            event_response = requests.get(EVENT_URL, headers=HEADERS)
            if event_response.status_code != 200:
                continue

            event_soup = BeautifulSoup(event_response.text, "lxml")

            title = event_soup.find("h1", itemprop="name")
            title = title.get_text(strip=True) if title else "No Title"

            image_tag = event_soup.find("a", class_="zoom_in")
            image_url = image_tag["href"] if image_tag and image_tag.has_attr("href") else "No Image"

            start_time_tag = event_soup.find("span", id="startdate", itemprop="startDate")
            end_time_tag = event_soup.find("span", id="startdate", itemprop="endDate")

            start_dt = date_parser.parse(start_time_tag["content"]) if start_time_tag and start_time_tag.has_attr("content") else None
            end_dt = date_parser.parse(end_time_tag["content"]) if end_time_tag and end_time_tag.has_attr("content") else None

            start_date, start_time = format_datetime_parts(start_dt) if start_dt else ("No Start Date", "No Start Time")
            end_date, end_time = format_datetime_parts(end_dt) if end_dt else (start_date, "No End Time")

            occurrence_tag = event_soup.find("span", style=lambda value: value and "color:#1997f3" in value)
            occurrences = occurrence_tag.get_text(strip=True) if occurrence_tag else "No Occurrences"
            print(start_date)
            print(end_date)
            print(start_time)
            print(end_time)
            print(occurrences)

            # Address
            location_name_tag = event_soup.find("span", itemprop="name")
            street_address_tag = event_soup.find("span", itemprop="streetAddress")
            city_tag = event_soup.find("span", itemprop="addressLocality")
            state_tag = event_soup.find("span", itemprop="addressRegion")
            postal_code_tag = event_soup.find("span", itemprop="postalCode")

            location_name = location_name_tag.get_text(strip=True) if location_name_tag else "No Location"
            street = street_address_tag.get_text(strip=True) if street_address_tag else ""
            city = city_tag.get_text(strip=True) if city_tag else ""
            state = state_tag.get_text(strip=True) if state_tag else ""
            postal = postal_code_tag.get_text(strip=True) if postal_code_tag else ""
            full_address = f"{street}, {city}, {state} {postal}".strip(", ")

            # Categories
            categories_tag = event_soup.find("b", string="Categories:")
            categories = categories_tag.find_next_sibling(string=True).strip() if categories_tag else "No Categories"

            admission_tag = event_soup.find("b", string="Admission:")
            admission = admission_tag.find_next_sibling("span").get_text(strip=True) if admission_tag else "No Admission Info"

            description_tag = event_soup.find("div", id="event_description")
            description = description_tag.get_text(strip=True) if description_tag else "No Description"

            # Skip if self-referential
            skip_event = False
            if description_tag:
                links = description_tag.find_all("a", href=True)
                for link in links:
                    if "https://www.thebostoncalendar.com/events/" in link["href"]:
                        skip_event = True
                        break

            if skip_event:
                continue

            event_list.append({
                "Event_Title": title,
                "Image_URL": image_url,
                "Start_Date": start_date,
                "End_Date": end_date,
                "Start_Time": start_time,
                "End_Time": end_time,
                "Occurrences": occurrences,
                "Location": location_name,
                "Full_Address": full_address,
                "Categories": categories,
                "Admission": admission,
                "Description": description,
                "Event_URL": EVENT_URL
            })

        df_events = pd.DataFrame(event_list)
        os.makedirs(TEMP_DIR, exist_ok=True)
        output_file = os.path.join(TEMP_DIR, WEBSITE_NAME, 'scraped_events.json')
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        df_events.to_json(output_file, orient='records')

        logger.info(f"Scraped {len(df_events)} events. Saved to {output_file}")
        context['ti'].xcom_push(key='events_data', value=output_file)
        return df_events

    except Exception as e:
        logger.error(f"Error scraping Boston Calendar: {str(e)}")
        raise
