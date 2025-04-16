from dotenv import load_dotenv
import streamlit as st
from auth.Login import login
from auth.Register import register_user
from auth.Logout import logout
from features.recommend_events import recommend_events
from features.search_events import search_events
from features.events_chatbot import events_chatbot

load_dotenv()

# Page Setup
st.set_page_config(page_title='EventLens App', page_icon='📅', layout='wide')

# Session Defaults
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'current_page' not in st.session_state:
    st.session_state.current_page = 'landing'

# Landing Page
def landing_page():
    st.title("📅 Welcome to EventLens")
    st.markdown("""
        EventLens is your smart assistant for discovering and managing events tailored just for you.
        
        ### 🔍 What can you do?
        - Get personalized **event recommendations**
        - Explore events using our **search tool**
        - Chat with our **event assistant**
    """)

    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔐 Login"):
            st.session_state.current_page = 'login'
            st.rerun()
    with col2:
        if st.button("📝 Register"):
            st.session_state.current_page = 'register'
            st.rerun()

# Page Routing
if st.session_state.current_page == 'landing':
    landing_page()
elif st.session_state.current_page == 'login':
    login()
elif st.session_state.current_page == 'register':
    register_user()
elif st.session_state.current_page == 'logout':
    logout()
    if 'logged_in' not in st.session_state:
        st.session_state.current_page = 'landing'
        st.rerun()
elif st.session_state.current_page == 'recommend':
    recommend_events()
elif st.session_state.current_page == 'search':
    search_events()
elif st.session_state.current_page == 'chatbot':
    events_chatbot()

# Sidebar Navigation for Logged-in Users
if st.session_state.get("logged_in", False):
    with st.sidebar:
        st.title(f"👋 Hi, {st.session_state.get('user_name', 'User')}")

        if st.button("🎯 Recommend Events"):
            st.session_state.current_page = 'recommend'
            st.rerun()

        if st.button("🔍 Search Events"):
            st.session_state.current_page = 'search'
            st.rerun()

        if st.button("💬 Event Chatbot"):
            st.session_state.current_page = 'chatbot'
            st.rerun()

        if st.button("🚪 Logout"):
            st.session_state.current_page = 'logout'
            st.rerun()