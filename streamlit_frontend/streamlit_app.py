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

    st.markdown(
        """
        <div style='text-align: center; padding-top: 20px;'>
            <h1 style='font-size: 3.2em;'>📅 Welcome to <span style="color:#4CAF50;">EventLens</span></h1>
            <p style='font-size: 1.25em; margin-top: 10px;'>
                Your smart assistant for discovering and managing events tailored just for you in <strong>Boston</strong>.
            </p>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.markdown(
        """
        <div style='text-align: center; margin-top: 10px;'>
            <p style='font-size: 1.1em; color: #aaa;'>🎉 Over <strong>5,000+</strong> live events happening across neighborhoods in Boston — concerts, markets, workshops, and more.</p>
            <p style='font-size: 1.05em; color: #ccc;'>EventLens helps you find the ones that truly matter to <em>you</em>.</p>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.markdown("---")

    st.markdown("### 🔍 What can you do?")
    st.markdown(
        """
        - 🧠 Get personalized **event recommendations** based on your interests
        - 🗺️ Explore what's happening in Boston using our **search assistant**
        - 💬 Chat with our smart **event discovery bot**
        """
    )

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