from streamlit_modal import Modal
import streamlit as st
import requests
import os
from PIL import Image, ImageOps
from io import BytesIO
from utils.s3_retreival import convert_s3_to_https

FAST_API_URL = os.getenv("FAST_API_URL")
DEFAULT_IMAGE_URL = "https://as2.ftcdn.net/jpg/02/16/94/65/1000_F_216946587_rmug8FCNgpDCPQlstiCJ0CAXJ2sqPRU7.jpg"

def load_and_resize_image(url, width=260, height=160):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            img = Image.open(BytesIO(response.content)).convert("RGB")
            resized = ImageOps.fit(img, (width, height), method=Image.Resampling.LANCZOS)
            return resized
    except Exception:
        pass
    return None

def search_events():
    st.markdown(
        """
        <div style="text-align:center; padding: 20px 0 10px;">
            <h1 style="font-size: 2.5em;">🔍 Explore Events in <span style="color:#4CAF50;">Boston</span></h1>
            <p style="font-size: 1.1em; color: #ccc;">Type a natural language query to discover events happening around the city.<br>
            Use keywords like <i>free music in the park</i> or <i>tech conferences this weekend</i>.</p>
        </div>
        """,
        unsafe_allow_html=True
    )

    modal = Modal(key="details_modal", title="📅 Event Details", padding=20)

    if "selected_event" not in st.session_state:
        st.session_state.selected_event = None
    if "search_results" not in st.session_state:
        st.session_state.search_results = []

    query = st.text_input("Enter your search query", placeholder="e.g. Free concerts this weekend in Boston")
    search_button = st.button("Search")

    if query and search_button:
        with st.spinner("Searching events..."):
            try:
                headers = {
                    "Authorization": f"Bearer {st.session_state.token}" 
                }
                response = requests.post(f"{FAST_API_URL}/events/search", json={"query": query}, headers=headers)

                if response.status_code == 200:
                    events = response.json().get("events", [])
                    #print(events)
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
                raw_url = event.get("IMAGE_S3_URL", "")
                image_url = convert_s3_to_https(raw_url) if raw_url else DEFAULT_IMAGE_URL
                image = load_and_resize_image(image_url)

                if image:
                    st.image(image, caption=event.get("EVENT_TITLE", "Untitled"))
                else:
                    fallback = load_and_resize_image(DEFAULT_IMAGE_URL)
                    st.image(fallback, caption="Image Unavailable")

                if st.button("More Details", key=f"details_{idx}"):
                    st.session_state.selected_event = event
                    modal.open()

    if modal.is_open():
        with modal.container():
            selected = st.session_state.get("selected_event")
            if selected:
                st.markdown(f"<h3 style='margin-bottom: 10px;'>📅 {selected.get('EVENT_TITLE', 'Event')} - Details</h3>", unsafe_allow_html=True)

                fields = [
                    ("START_DATE", "Start Date"),
                    ("END_DATE", "End Date"),
                    ("START_TIME", "Start Time"),
                    ("END_TIME", "End Time"),
                    ("FULL_ADDRESS", "Location"),
                    ("ADMISSION", "Admission"),
                    ("OCCURRENCES", "Occurrences"),
                ]
                for key, label in fields:
                    if selected.get(key):
                        st.markdown(f"- **{label}:** {selected[key]}")

                if selected.get("DESCRIPTION"):
                    desc = selected["DESCRIPTION"]
                    short_desc = " ".join(desc.split()[:60]) + "..."
                    st.markdown(f"**📝 Description:** {short_desc}")

                #if selected.get("CATEGORIES"):
                    #st.markdown(f"**🏷️ Categories:** {selected['CATEGORIES']}")

                # Event URL Button
                if selected.get("EVENT_URL"):
                    st.markdown(
                        f"""
                        <div style="margin-top: 15px;">
                            <a href="{selected['EVENT_URL']}" target="_blank">
                                <button style='
                                    background-color:#4CAF50;
                                    color:white;
                                    padding:10px 20px;
                                    border:none;
                                    border-radius:8px;
                                    cursor:pointer;
                                    font-size:16px;
                                '>🔗 Visit Event Page</button>
                            </a>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
