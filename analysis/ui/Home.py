import streamlit as st
import os
import sys
from pathlib import Path

# Add parent directory to Python path to correctly import modules
parent_dir = str(Path(__file__).parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from query.core.setup import setup_query_engine
from memory import ConversationMemory

# Page config
st.set_page_config(
    page_title="NC Soccer Hub - Chat",
    page_icon="⚽",
    layout="wide",
)

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []
if "memory_manager" not in st.session_state:
    st.session_state.memory_manager = ConversationMemory(
        storage_dir=Path(__file__).parent.parent / "conversations"
    )
if "session_id" not in st.session_state:
    # Check for existing session in environment variable
    env_session_id = os.environ.get("QUERY_SESSION")
    if env_session_id:
        st.session_state.session_id = env_session_id
        # Try to load the session if it exists
        session_file = os.path.join(st.session_state.memory_manager.storage_dir, f"{env_session_id}.json")
        if os.path.exists(session_file):
            st.session_state.memory_manager.load_session(env_session_id)
            print(f"Loaded existing session from environment: {env_session_id}")
    else:
        # Create a new session if none found in environment
        st.session_state.session_id = st.session_state.memory_manager.create_session()

# Set the session ID on the memory manager
st.session_state.memory_manager.session_id = st.session_state.session_id

# Export the session ID to environment for CLI integration
os.environ["QUERY_SESSION"] = st.session_state.session_id

# Title and description
st.title("⚽ NC Soccer Hub Chat")
st.markdown("""
Ask questions about soccer matches and team performance! Try queries like:
- "How did Key West club perform this month?"
- "Show me all matches for Key West FC"
- "Who are the top 10 teams by games played?"
""")

# Sidebar with session info and options
with st.sidebar:
    st.subheader("Session Information")
    st.text(f"Session ID: {st.session_state.session_id}")

    # Display history in sidebar
    st.sidebar.header("Conversation History")

    if 'memory_manager' in st.session_state and 'session_id' in st.session_state:
        history = st.session_state.memory_manager.get_session_history(st.session_state.session_id)

        # Display each Q&A pair in the sidebar if history exists
        if history and len(history) > 0:
            for query, response, timestamp in history:
                # Safety check for None values
                query = str(query) if query is not None else ""
                response = str(response) if response is not None else "No response available"
                timestamp = str(timestamp) if timestamp is not None else ""

                with st.sidebar.expander(f"Q: {query[:50]}..." if len(query) > 50 else f"Q: {query}"):
                    st.sidebar.text("Question:")
                    st.sidebar.markdown(query)
                    st.sidebar.text("Answer:")
                    st.sidebar.markdown(response)
        else:
            st.sidebar.info("No conversation history yet.")

    if st.sidebar.button("Clear Chat History"):
        st.session_state.messages = []
        st.session_state.memory_manager = ConversationMemory()
        st.session_state.session_id = st.session_state.memory_manager.create_session()
        st.rerun()

# Chat interface
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

if prompt := st.chat_input("Ask about soccer matches..."):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Get response from query engine
    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                # Initialize query engine with conversation history
                conversation_context = st.session_state.memory_manager.format_context(st.session_state.session_id)

                # Setup the query engine with appropriate parameters
                db_path = "matches.parquet"
                query_engine = setup_query_engine(
                    db_path=db_path,
                    verbose=True
                )

                # Process query and get response
                response = query_engine.query(prompt, memory=st.session_state.memory_manager)

                # Ensure response is a valid string
                if response is None:
                    response = "Sorry, I couldn't generate a response. Please try a different query."

                # Make sure response is converted to string safely
                response_str = str(response) if response is not None else "No response generated"

                # Store interaction in memory with context
                # Get memory context if it was set by the query engine
                memory_context = getattr(query_engine, 'memory_context', None)

                st.session_state.memory_manager.add_interaction(
                    session_id=st.session_state.session_id,
                    query=prompt,
                    response=response_str,
                    context=memory_context
                )

                # Display response
                st.markdown(response_str)

                # Add assistant message to chat history
                st.session_state.messages.append(
                    {"role": "assistant", "content": response_str}
                )

            except Exception as e:
                error_message = f"Error: {str(e)}"
                st.error(error_message)
                st.session_state.messages.append(
                    {"role": "assistant", "content": error_message}
                )