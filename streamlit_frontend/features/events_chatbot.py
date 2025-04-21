import streamlit as st

# ✅ MUST BE FIRST Streamlit command
st.set_page_config(page_title="EventLens Chatbot", layout="wide")

import requests
import os

FAST_API_URL = os.getenv("FAST_API_URL")

def events_chatbot():
    st.markdown("""
        <div style="text-align: center;">
            <h1>🤖 EventLens Chatbot</h1>
            <p style="font-size: 18px;">Get details about events, directions, weather, and reviews — all in one place!</p>
        </div>
    """, unsafe_allow_html=True)

    if 'token' not in st.session_state or not st.session_state.token:
        st.warning("Please login to use the chatbot.")
        return

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    for msg in st.session_state.chat_history:
        role = msg["role"]
        with st.chat_message(role):
            st.markdown(msg["content"])

    if prompt := st.chat_input("Ask me anything about events..."):
        st.chat_message("user").markdown(prompt)
        st.session_state.chat_history.append({"role": "user", "content": prompt})

        try:
            headers = {
                "Authorization": f"Bearer {st.session_state.token}"
            }

            payload = {
                "message": prompt,
                "history": st.session_state.chat_history
            }

            response = requests.post(
                f"{FAST_API_URL}/events/chat",
                headers=headers,
                json=payload
            )

            if response.status_code == 200:
                assistant_reply = response.json()["response"]
                st.chat_message("assistant").markdown(assistant_reply)
                st.session_state.chat_history.append({"role": "assistant", "content": assistant_reply})
            else:
                st.chat_message("assistant").markdown(f"⚠️ Server error: {response.status_code} - {response.text}")

        except Exception as e:
            st.chat_message("assistant").markdown(f"❌ Exception: {str(e)}")
