import streamlit as st

def init_chat_history():
    """Initialize chat history in session state if it doesn't exist."""
    if 'messages' not in st.session_state:
        st.session_state.messages = []

def add_message(role, content):
    """
    Add a message to the chat history.
    
    Args:
        role (str): Role of the sender ('user' or 'assistant')
        content (str): Message content
    """
    st.session_state.messages.append({"role": role, "content": content})

def display_chat_history():
    """Display all messages in the chat history."""
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])