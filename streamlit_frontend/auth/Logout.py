import streamlit as st
from typing import NoReturn

def logout() -> NoReturn:
    for key in ['logged_in', 'token', 'user_name']:
        if key in st.session_state:
            del st.session_state[key]

    st.session_state.current_page = 'landing'
