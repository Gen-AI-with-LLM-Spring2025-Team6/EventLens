import streamlit as st
import requests
import os

FAST_API_URL = os.getenv("FAST_API_URL")

def events_chatbot():
    st.markdown(
        """
        <div style="text-align:center; padding: 20px 0;">
            <h1 style="font-size: 3em;">🤖 EventLens Chatbot</h1>
            <p style="font-size: 1.2em; color: #ccc;">
                Get instant help with events in Boston!<br>
                This assistant can help you with <strong>event info, directions, weather, and reviews</strong> — all in one place.
            </p>
        </div>
        """,
        unsafe_allow_html=True
    )

    if 'token' not in st.session_state or not st.session_state.token:
        st.error("🔒 Please log in to access the chatbot.")
        return

    # Initialize message history
    if "chat_messages" not in st.session_state:
        st.session_state.chat_messages = []

    # Display chat history
    for message in st.session_state.chat_messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Capture new message
    if prompt := st.chat_input("Ask me anything about events..."):
        # Show user message in UI
        st.chat_message("user").markdown(prompt)
        st.session_state.chat_messages.append({"role": "user", "content": prompt})

        # Send to backend with chat history
        payload = {
            "messages": st.session_state.chat_messages
        }

        try:
            headers = {"Authorization": f"Bearer {st.session_state.token}"}
            with st.spinner("Thinking..."):
                response = requests.post(f"{FAST_API_URL}/events/chat", json=payload, headers=headers)
                if response.status_code == 200:
                    reply = response.json().get("answer", "🤔 Sorry, I couldn't find anything useful.")
                else:
                    reply = f"⚠️ Server error: {response.status_code}"
        except Exception as e:
            reply = f"❌ Error: {str(e)}"

        # Show assistant reply
        st.chat_message("assistant").markdown(reply)
        st.session_state.chat_messages.append({"role": "assistant", "content": reply})
