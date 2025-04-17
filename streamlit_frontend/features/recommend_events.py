import streamlit as st
import requests
import os
from PIL import Image, ImageOps
from io import BytesIO
from utils.s3_retreival import convert_s3_to_https

FAST_API_URL = os.getenv("FAST_API_URL")
DEFAULT_IMAGE_URL = "https://as2.ftcdn.net/jpg/02/16/94/65/1000_F_216946587_rmug8FCNgpDCPQlstiCJ0CAXJ2sqPRU7.jpg"

def load_and_resize_image(url, width=320, height=220):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            img = Image.open(BytesIO(response.content)).convert("RGB")
            resized = ImageOps.fit(img, (width, height), method=Image.Resampling.LANCZOS)
            return resized
    except Exception:
        pass
    return None

def recommend_events():
    st.markdown(
        """
        <div style="text-align:center; padding: 20px 0 10px;">
            <h1 style="font-size: 3em;">🎯 Welcome to <span style="color:#4CAF50;">EventLens</span></h1>
            <p style="font-size: 1.2em; color: #ccc;">Discover events personalized just for you!<br>
            Our system uses your interests and previous activity to generate the best event picks across Boston.</p>
        </div>
        """,
        unsafe_allow_html=True
    )

    if 'token' not in st.session_state or not st.session_state.token:
        st.error("🔒 Please log in to view your recommendations.")
        return

    # Initialize recommendation results state
    if "recommendation_results" not in st.session_state:
        st.session_state.recommendation_results = []

    # Generate button
    if st.button("✨ Generate Recommendations"):
        try:
            headers = {"Authorization": f"Bearer {st.session_state.token}"}
            with st.spinner("Fetching recommendations..."):
                response = requests.get(f"{FAST_API_URL}/events/recommend", headers=headers)
                if response.status_code == 200:
                    events = response.json().get("events", [])
                    st.session_state.recommendation_results = events
                    if not events:
                        st.warning("🚫 No recommendations available at the moment.")
                        return
                else:
                    st.error(f"Failed to fetch recommendations: {response.status_code} - {response.text}")
                    return
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")
            return

    # Show recommendations if available
    if st.session_state.recommendation_results:
        for idx, event in enumerate(st.session_state.recommendation_results):
            with st.container():
                st.markdown(f"### 📌 {event.get('EVENT_TITLE', 'Untitled Event')}")

                image_url = convert_s3_to_https(event.get("IMAGE_S3_URL", "")) or DEFAULT_IMAGE_URL
                image = load_and_resize_image(image_url)
                if image:
                    st.image(image, caption="", use_container_width=False)
                else:
                    fallback = load_and_resize_image(DEFAULT_IMAGE_URL)
                    st.image(fallback, caption="Image Unavailable", use_container_width=False)

                st.markdown("#### 🗓️ Event Details")
                fields = [
                    ("START_DATE", "Start Date"),
                    ("START_TIME", "Start Time"),
                    ("END_DATE", "End Date"),
                    ("END_TIME", "End Time"),
                    ("OCCURRENCES", "Occurrences"),
                    ("FULL_ADDRESS", "Address"),
                    ("LOCATION", "Location"),
                    ("ADMISSION", "Admission")
                ]
                details = ""
                for key, label in fields:
                    value = event.get(key)
                    if value:
                        details += f"<li><strong>{label}:</strong> {value}</li>\n"
                st.markdown(f"<ul style='margin-bottom: 8px;'>{details}</ul>", unsafe_allow_html=True)

                if event.get("DESCRIPTION"):
                    desc = event["DESCRIPTION"]
                    short_desc = " ".join(desc.split()[:50]) + "..."
                    st.markdown(f"📄 <strong>Description:</strong> {short_desc}", unsafe_allow_html=True)

                #if event.get("CATEGORIES"):
                    #st.markdown(f"🏷️ <strong>Categories:</strong> {event['CATEGORIES']}", unsafe_allow_html=True)

                event_url = event.get("EVENT_URL")
                if event_url:
                    st.markdown(
                        f"""
                        <div style="margin-top: 12px;">
                            <a href="{event_url}" target="_blank">
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

                st.markdown("<hr style='margin-top: 30px;'>", unsafe_allow_html=True)
