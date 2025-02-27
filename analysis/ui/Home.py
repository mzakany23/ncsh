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
        db_path=Path(__file__).parent / "conversation_history.db"
    )
if "session_id" not in st.session_state:
    st.session_state.session_id = st.session_state.memory_manager.create_session()

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

    # Show conversation history in sidebar
    st.subheader("Conversation History")
    history = st.session_state.memory_manager.get_session_history(st.session_state.session_id)
    if history:
        for query, response, _ in history:
            with st.expander(f"Q: {query[:50]}..."):
                st.text("Question:")
                st.markdown(query)
                st.text("Answer:")
                st.markdown(response)

    if st.button("Clear Chat History"):
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

                # Store interaction in memory with context
                # Get memory context if it was set by the query engine
                memory_context = getattr(query_engine, 'memory_context', None)

                st.session_state.memory_manager.add_interaction(
                    session_id=st.session_state.session_id,
                    query=prompt,
                    response=str(response),
                    context=memory_context
                )

                # Display response
                st.markdown(response)

                # Add assistant message to chat history
                st.session_state.messages.append(
                    {"role": "assistant", "content": str(response)}
                )

            except Exception as e:
                error_message = f"Error: {str(e)}"
                st.error(error_message)
                st.session_state.messages.append(
                    {"role": "assistant", "content": error_message}
                )