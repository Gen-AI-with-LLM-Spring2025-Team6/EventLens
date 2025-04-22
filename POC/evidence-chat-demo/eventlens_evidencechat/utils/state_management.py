import streamlit as st
import os

def initialize_session_state():
    """Initialize all session state variables."""
    if 'initialized' not in st.session_state:
        st.session_state.initialized = True
        st.session_state.snowflake_connected = False
        st.session_state.snowflake_conn = None
        
        # Load default values from environment variables
        st.session_state.default_table = os.getenv("SNOWFLAKE_TABLE_NAME", "")
        st.session_state.default_embedding_col = os.getenv("SNOWFLAKE_EMBEDDING_COLUMN", "vector_embedding")
        st.session_state.default_text_col = os.getenv("SNOWFLAKE_TEXT_COLUMN", "combined_text")
        
        # These will store the actual values used in queries
        st.session_state.table_name_value = st.session_state.default_table
        st.session_state.embedding_col_value = st.session_state.default_embedding_col
        st.session_state.text_col_value = st.session_state.default_text_col
        
        # Toggle for showing raw chunks
        st.session_state.show_raw_chunks = True