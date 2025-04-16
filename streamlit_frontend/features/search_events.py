from streamlit_modal import Modal
import streamlit as st
import requests
import os
from PIL import Image
from io import BytesIO
from utils.s3_retreival import convert_s3_to_https

FAST_API_URL = os.getenv("FAST_API_URL")
DEFAULT_IMAGE_URL = "https://as2.ftcdn.net/jpg/02/16/94/65/1000_F_216946587_rmug8FCNgpDCPQlstiCJ0CAXJ2sqPRU7.jpg"

def search_events():
    st.title("🔍 Search Events")
    st.info("Search events using natural language queries like topics, locations, or dates.")

    modal = Modal(key="details_modal", title="📅 Event Details", padding=20)

    # Initialize session state
    if "selected_event" not in st.session_state:
        st.session_state.selected_event = None
    if "search_results" not in st.session_state:
        st.session_state.search_results = []

    query = st.text_input("Enter your search query", placeholder="e.g. Free concerts this weekend in Boston")
    search_button = st.button("Search")

    if query and search_button:
        with st.spinner("Searching events..."):
            try:
                response = requests.post(f"{FAST_API_URL}/events/search", json={"query": query})
                if response.status_code == 200:
                    events = response.json().get("events", [])
                    st.session_state.search_results = events
                    if not events:
                        st.warning("No events matched your search.")
                        return
                else:
                    st.error(f"Failed to search events: {response.status_code} - {response.text}")
                    return
            except Exception as e:
                st.error(f"An error occurred during search: {str(e)}")
                return

    events = st.session_state.get("search_results", [])

    if events:
        rows = [st.columns(4) for _ in range((len(events) + 3) // 4)]

        for idx, event in enumerate(events):
            col = rows[idx // 4][idx % 4]
            with col:
                image_url = event.get("IMAGE_S3_URL", "")
                image_url = convert_s3_to_https(image_url) if image_url else DEFAULT_IMAGE_URL

                try:
                    img_response = requests.get(image_url)
                    if img_response.status_code == 200:
                        image = Image.open(BytesIO(img_response.content))
                        st.image(image, width=180, caption=event.get("EVENT_TITLE", "Untitled"))
                    else:
                        st.image(DEFAULT_IMAGE_URL, width=180, caption="Default Image")
                except:
                    st.image(DEFAULT_IMAGE_URL, width=180, caption="Image Failed to Load")

                if st.button("More Details", key=f"details_{idx}"):
                    st.session_state.selected_event = event
                    modal.open()

    if modal.is_open():
        with modal.container():
            selected = st.session_state.get("selected_event")
            if selected:
                st.subheader(f"📅 {selected.get('EVENT_TITLE', 'Event')} - Details")
                st.markdown(f"**Date:** {selected.get('START_DATE')} to {selected.get('END_DATE')}")
                st.markdown(f"**Time:** {selected.get('START_TIME')} to {selected.get('END_TIME')}")
                st.markdown(f"**Location:** {selected.get('FULL_ADDRESS')}")
                st.markdown(f"**Admission:** {selected.get('ADMISSION')}")
                st.markdown(f"**Categories:** {selected.get('CATEGORIES')}")
                st.markdown(f"**Occurrences:** {selected.get('OCCURRENCES')}")
                st.markdown(f"**Description:** {selected.get('DESCRIPTION')}")
                if selected.get("EVENT_URL"):
                    st.markdown(f"[🔗 Event Link]({selected['EVENT_URL']})", unsafe_allow_html=True)
