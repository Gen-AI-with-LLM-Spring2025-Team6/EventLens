import streamlit as st
import requests
import os
import time
from requests.exceptions import RequestException
from dotenv import load_dotenv
from utils.validate_fields import is_valid_username, is_valid_email, is_valid_password

load_dotenv()
FAST_API_URL = os.getenv("FAST_API_URL")

def register_user():
    if st.button("← Back", type="secondary"):
        st.session_state.current_page = 'landing'
        st.rerun()

    st.title("📅 EventLens")
    st.header('Register')

    with st.expander("Registration Requirements"):
        st.markdown("""
        **Username:** 3-20 characters, starts with a letter, letters/numbers/_/-  
        **Password:** Min 8 chars, includes uppercase, lowercase, number, special char  
        **Email:** Must be a valid format (e.g., user@domain.com)
        """)

    with st.form(key='register_form'):
        username = st.text_input('Username')
        email = st.text_input('Email')
        password = st.text_input('Password', type='password')

        interests_options = [
            "Arts & Culture", "Food & Drink", "Nightlife & Parties", "Date Ideas",
            "Kids & Families", "Professional & Networking / Educational",
            "Sports & Active Life", "Community & Social Good", 
            "Fairs, Festivals & Shopping", "Other"
        ]
        interests = st.multiselect("Select Your Interests (at least one required)", interests_options)

        submit_button = st.form_submit_button(label='Register')

        if submit_button:
            errors = []
            if not username or not is_valid_username(username):
                errors.append("Invalid or missing username.")
            if not email or not is_valid_email(email):
                errors.append("Invalid or missing email.")
            valid_pw, pw_msg = is_valid_password(password)
            if not password or not valid_pw:
                errors.append(pw_msg)
            if not interests:
                errors.append("Please select at least one interest.")

            if errors:
                for error in errors:
                    st.error(error)
                return

            try:
                with st.spinner('Registering...'):
                    response = requests.post(
                        f"{FAST_API_URL}/auth/register",
                        json={
                            "username": username,
                            "email": email,
                            "password": password,
                            "interests": interests
                        },
                        timeout=10
                    )

                if response.status_code == 201:
                    st.success("Registration successful!")
                    countdown_placeholder = st.empty()
                    for seconds in range(3, 0, -1):
                        countdown_placeholder.info(f"Redirecting to login in {seconds} seconds...")
                        time.sleep(1)
                    countdown_placeholder.empty()
                    st.session_state.current_page = 'login'
                    st.rerun()
                else:
                    st.error(response.json().get('detail', 'Registration failed.'))
            except RequestException:
                st.error("Unable to connect to the server.")
            except Exception:
                st.error("Unexpected error during registration.")
