import streamlit as st
import requests
import os
from PIL import Image
from io import BytesIO
from utils.s3_retreival import convert_s3_to_https

FAST_API_URL = os.getenv("FAST_API_URL")

def recommend_events():
    st.title("🎯 Recommended Events for You")
    st.info("These are events tailored to your interests!")

    if 'token' not in st.session_state or not st.session_state.token:
        st.error("Please log in to view recommendations.")
        return

    try:
        headers = {
            "Authorization": f"Bearer {st.session_state.token}"
        }
        response = requests.get(f"{FAST_API_URL}/events/recommend", headers=headers)

        if response.status_code == 200:
            events = response.json().get("events", [])
            if not events:
                st.warning("No recommendations currently available.")
                return

            for idx, event in enumerate(events):
                with st.container():
                    st.subheader(event.get("EVENT_TITLE", "Untitled Event"))

                    # Optional Image (fetch and render from URL)
                    image_url = event.get("IMAGE_S3_URL")
                    image_url = convert_s3_to_https(image_url)
                    if image_url:
                        try:
                            response = requests.get(image_url)
                            if response.status_code == 200:
                                image = Image.open(BytesIO(response.content))
                                st.image(image, width=600, caption="")
                            else:
                                st.warning(f"Image not accessible: {image_url}")
                        except Exception as e:
                            st.warning(f"Image failed to load: {image_url}")

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
                    field_display = [f"**{label}:** {event.get(key)}" for key, label in fields if event.get(key)]
                    if field_display:
                        st.markdown(" | ".join(field_display))

                    if event.get("DESCRIPTION"):
                        st.markdown(f"**Description:** {event['DESCRIPTION']}")

                    event_url = event.get("EVENT_URL")
                    if event_url:
                        st.markdown(
                            f"""
                            <a href="{event_url}" target="_blank">
                                <button style='
                                    background-color:#4CAF50;
                                    color:white;
                                    padding:10px 20px;
                                    margin-top:10px;
                                    border:none;
                                    border-radius:8px;
                                    cursor:pointer;
                                    font-size:16px;
                                '>🔗 Visit Event Page</button>
                            </a>
                            """,
                            unsafe_allow_html=True
                        )

                    st.markdown("---")
        else:
            st.error(f"Failed to fetch recommendations: {response.status_code} - {response.text}")

    except Exception as e:
        st.error(f"An error occurred: {str(e)}")