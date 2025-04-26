import streamlit as st
import os
import requests
from requests.exceptions import RequestException
from dotenv import load_dotenv

load_dotenv()
FAST_API_URL = os.getenv("FAST_API_URL")

def login():
    if st.button("← Back", type="secondary"):
        st.session_state.current_page = 'landing'
        st.rerun()

    st.title("📅 EventLens")
    st.header('Login')

    if 'token' not in st.session_state:
        st.session_state.token = None
    if 'user_name' not in st.session_state:
        st.session_state.user_name = None

    with st.form(key='login_form'):
        username = st.text_input('Username', placeholder='Enter your username')
        password = st.text_input('Password', type='password', placeholder='Enter your password')
        submit_button = st.form_submit_button(label='Login')

        if submit_button:
            if not username or not password:
                st.error("Username and password are required.")
                return

            try:
                with st.spinner('Logging in...'):
                    response = requests.post(
                        f"{FAST_API_URL}/auth/login",
                        json={"username": username, "password": password},
                        timeout=10
                    )

                if response.status_code == 200:
                    data = response.json()
                    token = data.get("access_token")
                    username = data.get("username")
                    if token and username:
                        st.success("Login successful!")
                        st.session_state.token = token
                        st.session_state.user_name = username
                        st.session_state.logged_in = True
                        st.session_state.current_page = 'recommend'
                        st.rerun()
                    else:
                        st.error("Missing token or username.")
                elif response.status_code == 400:
                    st.error(response.json().get('detail', 'Invalid credentials.'))
                elif response.status_code == 401:
                    st.error("Unauthorized. Please check your credentials.")
                elif response.status_code == 404:
                    st.error("User not found. Please register first.")
                elif response.status_code == 500:
                    st.error("Check your username and password.")
                elif response.status_code == 402:
                    st.error("Check your username and password.")
                else:
                    st.error(f"Login failed. Status code: {response.status_code}")

            except RequestException:
                st.error("Unable to reach the server. Please try again.")
            except Exception:
                st.error("Unexpected error during login.")
