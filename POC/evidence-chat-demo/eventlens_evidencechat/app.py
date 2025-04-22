import streamlit as st
import os
from dotenv import load_dotenv

# Import modules
from database.snowflake_connector import init_snowflake_connection
from utils.chat_utils import init_chat_history, add_message, display_chat_history
from utils.state_management import initialize_session_state
from services.event_service import analyze_user_query, search_events_per_query, generate_event_recommendations

# Load environment variables
load_dotenv()

def main():
    # Initialize session state variables
    initialize_session_state()
    
    st.set_page_config(page_title="EventLens - Discover Your Perfect Events", layout="wide")
    st.title("EventLens: Your AI-Powered Event Discovery Assistant")
    st.markdown("*Tell us what you're looking for, and we'll find the perfect events for you!*")
    
    # Check if environment variables are loaded
    snowflake_env_loaded = os.getenv("SNOWFLAKE_ACCOUNT") is not None
    
    # Sidebar: Configuration
    with st.sidebar:
        st.header("EventLens Settings")
        
        # Snowflake connection settings
        with st.expander("Database Connection", expanded=not (st.session_state.snowflake_connected or snowflake_env_loaded)):
            if snowflake_env_loaded:
                st.success("Database credentials loaded successfully")
                
                # Table settings with callback functions to update values
                def update_table():
                    st.session_state.table_name_value = st.session_state.table_input
                    
                def update_embedding():
                    st.session_state.embedding_col_value = st.session_state.embedding_input
                    
                def update_text():
                    st.session_state.text_col_value = st.session_state.text_input
                
                # Create widgets with unique keys and callbacks
                st.text_input(
                    "Events Table", 
                    value=st.session_state.default_table,
                    key="table_input",
                    on_change=update_table
                )
                
                st.text_input(
                    "Event Embedding Column", 
                    value=st.session_state.default_embedding_col,
                    key="embedding_input",
                    on_change=update_embedding
                )
                
                st.text_input(
                    "Event Text Column", 
                    value=st.session_state.default_text_col,
                    key="text_input",
                    on_change=update_text
                )
            else:
                st.warning("Database credentials not found. Please connect manually.")
                # Manual connection UI
                snowflake_account = st.text_input("Snowflake Account", key="sf_account")
                snowflake_user = st.text_input("Snowflake Username", key="sf_user")
                snowflake_password = st.text_input("Snowflake Password", type="password", key="sf_pass")
                snowflake_warehouse = st.text_input("Warehouse", key="sf_warehouse")
                snowflake_database = st.text_input("Database", key="sf_db")
                snowflake_schema = st.text_input("Schema", key="sf_schema")
                
                # Table settings for manual connection
                st.session_state.table_name_value = st.text_input("Events Table", key="table_name_manual")
                st.session_state.embedding_col_value = st.text_input("Event Embedding Column", value="vector_embedding", key="embed_col_manual")
                st.session_state.text_col_value = st.text_input("Event Text Column", value="combined_text", key="text_col_manual")
            
            # Connect button
            if st.button("Connect to Event Database"):
                if snowflake_env_loaded:
                    conn = init_snowflake_connection()
                else:
                    # Use manually entered credentials
                    conn = init_snowflake_connection(
                        user=st.session_state.get("sf_user"),
                        password=st.session_state.get("sf_pass"),
                        account=st.session_state.get("sf_account"),
                        warehouse=st.session_state.get("sf_warehouse"),
                        database=st.session_state.get("sf_db"),
                        schema=st.session_state.get("sf_schema")
                    )
                
                if conn:
                    st.session_state.snowflake_conn = conn
                    st.session_state.snowflake_connected = True
                    st.success("Connected to event database successfully!")
        
        # If credentials are loaded from .env but not connected yet, attempt connection
        if snowflake_env_loaded and not st.session_state.snowflake_connected:
            conn = init_snowflake_connection()
            if conn:
                st.session_state.snowflake_conn = conn
                st.session_state.snowflake_connected = True
                st.success("Connected to event database automatically!")
        
        # AI configuration
        api_key = st.text_input(
            "Enter OpenAI API Key", 
            value=os.getenv("OPENAI_API_KEY", ""),
            type="password",
            key="api_key_input"
        )
        
        model_name = st.selectbox(
            "Select AI Model",
            ["gpt-4o-mini", "gpt-4o", "gpt-4-turbo", "gpt-3.5-turbo"],
            index=0,
            key="model_selector"
        )
        
        # Number of results to retrieve
        events_per_query = st.slider(
            "Events to retrieve per search query", 
            min_value=1, 
            max_value=5, 
            value=3,
            key="events_slider"
        )
        
        # Toggle for showing raw chunks
        show_raw_chunks = st.checkbox(
            "Show raw event chunks", 
            value=st.session_state.show_raw_chunks,
            key="show_chunks_toggle"
        )
        st.session_state.show_raw_chunks = show_raw_chunks
        
        # Store API settings in session state
        st.session_state.api_key = api_key
        st.session_state.model_name = model_name
        st.session_state.events_per_query = events_per_query
    
    # Main chat interface
    init_chat_history()
    display_chat_history()
    
    # Chat input
    if not st.session_state.snowflake_connected:
        st.warning("Please connect to the event database using the sidebar before continuing.")
    else:
        if question := st.chat_input("What kind of events are you looking for? (e.g., 'Weekend concerts in Boston' or 'Family-friendly events next week')"):
            # Add user message to chat
            add_message("user", question)
            st.chat_message("user").markdown(question)
            
            # Add assistant response with thinking indicators
            with st.chat_message("assistant"):
                thinking_placeholder = st.empty()
                thinking_placeholder.markdown("🧠 Analyzing your request...")
                
                # Step 1: Generate search queries from user request
                search_queries = analyze_user_query(
                    question,
                    st.session_state.api_key,
                    st.session_state.model_name
                )
                
                if not search_queries:
                    error_msg = "😕 I'm having trouble understanding your request. Could you try rephrasing it?"
                    thinking_placeholder.markdown(error_msg)
                    add_message("assistant", error_msg)
                    return
                
                # Display the search queries being used
                query_text = "\n".join([f"{i+1}. {query}" for i, query in enumerate(search_queries)])
                thinking_placeholder.markdown(f"🔍 Here's what I'm looking for based on your request:\n{query_text}\n\nSearching for events...")
                
                # Step 2: Search for events using each query
                event_results, query_results_map = search_events_per_query(
                    search_queries,
                    st.session_state.snowflake_conn,
                    st.session_state.table_name_value,
                    st.session_state.embedding_col_value,
                    st.session_state.text_col_value,
                    limit_per_query=st.session_state.events_per_query
                )
                
                if not event_results:
                    error_msg = "😕 I couldn't find any events matching your criteria. Perhaps try broadening your search?"
                    thinking_placeholder.markdown(error_msg)
                    add_message("assistant", error_msg)
                    return
                
                # Show raw text chunks if enabled
                if st.session_state.show_raw_chunks:
                    # Create a new expander for each search query and its results
                    thinking_placeholder.markdown(f"✨ Found {len(event_results)} events that might interest you. Here are the raw details:")
                    
                    # Create expandable sections for each query's results
                    for query_idx, query in enumerate(search_queries):
                        with st.expander(f"Search Query {query_idx+1}: '{query}'"):
                            if query in query_results_map and query_results_map[query]:
                                for i, result in enumerate(query_results_map[query]):
                                    event_id = result[0]
                                    text = result[1]
                                    similarity = result[2]
                                    st.markdown(f"**Event {i+1}** (Event ID: {event_id}, Similarity: {similarity:.4f})")
                                    st.text_area(f"Raw Text - Query {query_idx+1}, Event {i+1}", 
                                                value=text, 
                                                height=150, 
                                                key=f"raw_{query_idx}_{i}")
                            else:
                                st.markdown("No unique results found for this query.")
                    
                    thinking_placeholder.markdown(f"Preparing your recommendations based on these events...")
                else:
                    thinking_placeholder.markdown(f"✨ Found {len(event_results)} events that might interest you. Preparing your recommendations...")
                
                # Step 3: Generate final recommendations
                final_recommendations = generate_event_recommendations(
                    question,
                    search_queries,
                    event_results,
                    st.session_state.api_key,
                    st.session_state.model_name
                )
                
                # Create expandable section for full retrieved context
                final_output = "### My Search Strategy\n"
                for i, query in enumerate(search_queries):
                    final_output += f"{i+1}. {query}\n"
                
                if st.session_state.show_raw_chunks:
                    final_output += "\n### Retrieved Event Data\n"
                    final_output += f"I found {len(event_results)} relevant events from the database. You can expand each search query above to see the specific events found.\n"
                
                final_output += "\n### Recommended Events\n" + final_recommendations
                
                # Create a final expandable section with debug info
                with st.expander("Debug Information"):
                    st.markdown("### Retrieved Event Data")
                    for i, result in enumerate(event_results):
                        event_id = result[0]
                        text = result[1]
                        similarity = result[2]
                        st.markdown(f"**Event {i+1}** (Event ID: {event_id}, Similarity: {similarity:.4f})")
                        st.text_area(f"Raw text for event {i+1}", text, height=150)
                
                # Display the final output
                thinking_placeholder.markdown(final_output)
                add_message("assistant", final_output)

if __name__ == "__main__":
    main()