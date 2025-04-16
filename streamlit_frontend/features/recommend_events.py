import streamlit as st
import requests
import os

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

                    # Optional Image (as URL)
                    image_url = event.get("IMAGE_S3_URL")
                    if image_url:
                        st.markdown(f"📷 **Image URL:** {image_url}")
                        # Optionally uncomment to preview:
                        # st.image(image_url, width=400)

                    # Display event fields only if they exist
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

                    # Description
                    if event.get("DESCRIPTION"):
                        st.markdown(f"**Description:** {event['DESCRIPTION']}")

                    # Event URL button
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
