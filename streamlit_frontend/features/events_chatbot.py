import streamlit as st
import requests
import os
import time

#st.set_page_config(page_title="EventLens Chatbot", layout="wide")

FAST_API_URL = os.getenv("FAST_API_URL")

def events_chatbot():
    st.markdown("""
        <div style="text-align: center; padding: 20px 0;">
            <h1 style="font-size: 2.6em;">🤖 EventLens <span style="color:#4CAF50;">Chatbot</span></h1>
            <p style="font-size: 1.2em; color: #ccc;">
                Your all-in-one conversational assistant for <span style="color:#4CAF50;">Boston</span> events.<br>
                Ask about event details, directions, weather forecasts, and user reviews — all in one place!
            </p>
        </div>
    """, unsafe_allow_html=True)

    if 'token' not in st.session_state or not st.session_state.token:
        st.warning("🔒 Please login to use the chatbot.")
        return

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    # Render chat history
    for msg in st.session_state.chat_history:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # New prompt input
    if prompt := st.chat_input("Ask me anything about events in Boston..."):
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

            # Stage 1: Analyzing spinner
            with st.spinner("🧠 Analyzing your query..."):
                time.sleep(1)

            # Stage 2: Fetching results spinner
            with st.spinner("🔍 Fetching results..."):
                response = requests.post(
                    f"{FAST_API_URL}/events/chat",
                    headers=headers,
                    json=payload
                )

            if response.status_code == 200:
                assistant_reply = response.json()["response"]
                with st.chat_message("assistant"):
                    st.markdown(assistant_reply)
                st.session_state.chat_history.append({"role": "assistant", "content": assistant_reply})
            else:
                st.chat_message("assistant").markdown(
                    f"⚠️ Server error: {response.status_code} - {response.text}"
                )

        except Exception as e:
            st.chat_message("assistant").markdown(f"❌ Exception occurred: `{str(e)}`")
