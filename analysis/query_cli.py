#!/usr/bin/env python
"""
Command Line Interface for the Query Engine.

This script provides a simple CLI for running queries against the soccer matches database.
It leverages the refactored query engine to process natural language queries and return responses.
"""

import os
import sys
import argparse
from datetime import datetime
import gc

# Add the parent directory to the path so we can import our modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import from our refactored modules
from analysis.query.core.setup import setup_query_engine, run_query
from analysis.memory import ConversationMemory
from analysis.query.core.engine import QueryEngine

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Soccer Query Engine CLI")
    parser.add_argument(
        "query", nargs="?", default=None,
        help="The query to run. If not provided, interactive mode will be used."
    )
    parser.add_argument(
        "--db", "-d", default="matches.parquet",
        help="Path to the matches database file (default: matches.parquet)"
    )
    parser.add_argument(
        "--model", "-m", default="claude-3-7-sonnet-latest",
        help="Anthropic model to use (default: claude-3-7-sonnet-latest)"
    )
    parser.add_argument(
        "--api-key", "-k", default=None,
        help="Anthropic API key (defaults to ANTHROPIC_API_KEY environment variable)"
    )
    parser.add_argument(
        "--api-base", "-b", default=None,
        help="Anthropic API base URL (defaults to ANTHROPIC_API_BASE environment variable)"
    )
    parser.add_argument(
        "--session", "-s", default=None,
        help="Session ID for conversation continuity (default: generated from timestamp)"
    )
    parser.add_argument(
        "--interactive", "-i", action="store_true",
        help="Run in interactive mode (default if no query is provided)"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable verbose logging"
    )
    parser.add_argument(
        "--list-sessions", "-l", action="store_true",
        help="List all available sessions"
    )
    return parser.parse_args()

def interactive_mode(args):
    """Run the query engine in interactive mode."""
    print("\n🔍 Soccer Query Engine - Interactive Mode 🔍")
    print("Type 'exit', 'quit', or Ctrl+C to exit")
    print("Type 'reset' to reset the conversation memory")
    print("-" * 50)

    # Create a properly structured memory manager using ConversationMemory
    memory_manager = ConversationMemory()

    # Ensure the conversations directory exists
    os.makedirs(memory_manager.storage_dir, exist_ok=True)

    # Create a session ID if not provided
    session_id = args.session
    if not session_id:
        session_id = memory_manager.create_session()
        print(f"Created new session: {session_id}")
    else:
        # Try to load existing session if provided
        # First check if the session file exists
        session_file = os.path.join(memory_manager.storage_dir, f"{session_id}.json")
        if os.path.exists(session_file):
            memory_manager.load_session(session_id)
            print(f"Loaded existing session: {session_id}")
        else:
            # For custom session IDs, we'll create a new session with exactly that ID
            # This is different from the default behavior that generates timestamp-based IDs
            memory_manager.sessions[session_id] = []
            memory_manager._save_session(session_id)
            print(f"Created new custom session: {session_id}")

    print(f"Session ID: {session_id}")

    # Set up the query engine
    engine = setup_query_engine(
        db_path=args.db,
        model_name=args.model,
        api_key=args.api_key,
        api_base=args.api_base,
        verbose=args.verbose
    )

    try:
        while True:
            # Get user input
            query = input("\n> ")

            # Check for exit commands
            if query.lower() in ["exit", "quit"]:
                print("Exiting...")
                break

            # Check for reset command
            if query.lower() == "reset":
                engine.reset_memory()
                memory_manager = ConversationMemory()
                session_id = memory_manager.create_session()
                print("Conversation memory reset.")
                continue

            # Process the query
            if query.strip():
                print("\nProcessing query...")

                # Pass the memory manager to the query engine
                response = engine.query(query, memory=memory_manager)

                # Store interaction in memory with context from the query engine
                memory_context = getattr(engine, 'memory_context', None)
                memory_manager.add_interaction(
                    session_id=session_id,
                    query=query,
                    response=str(response),
                    context=memory_context
                )

                print("\nResponse:")
                print("-" * 50)
                print(response)
                print("-" * 50)

    except KeyboardInterrupt:
        print("\nExiting...")

def main():
    """Main entry point."""
    args = parse_args()

    # If requested, list all available sessions and exit
    if args.list_sessions:
        memory_manager = ConversationMemory()
        sessions = memory_manager.list_sessions()
        if sessions:
            print("Available sessions:")
            for session in sessions:
                print(f"  - {session}")
        else:
            print("No sessions found.")
        return

    # Run in interactive mode if requested or if no query is provided
    if args.interactive or args.query is None:
        interactive_mode(args)
        return

    # For single queries, create a memory manager just for this query
    memory_manager = ConversationMemory()

    # Ensure the conversations directory exists
    os.makedirs(memory_manager.storage_dir, exist_ok=True)

    # Handle session ID
    session_id = args.session
    if not session_id:
        session_id = memory_manager.create_session()
        print(f"Created new session: {session_id}")
    else:
        # Try to load existing session if provided
        session_file = os.path.join(memory_manager.storage_dir, f"{session_id}.json")
        if os.path.exists(session_file):
            memory_manager.load_session(session_id)
            print(f"Loaded existing session: {session_id}")
        else:
            # For custom session IDs, create a new session with exactly that ID
            memory_manager.sessions[session_id] = []
            memory_manager._save_session(session_id)
            print(f"Created new custom session: {session_id}")

    # Run a single query with memory context
    response = run_query(
        query=args.query,
        db_path=args.db,
        model_name=args.model,
        api_key=args.api_key,
        api_base=args.api_base,
        memory=memory_manager,
        verbose=args.verbose
    )

    # Get memory context from the last query engine that was created
    # Import the necessary module to access the engine
    # Find the current instance
    engine_instances = [obj for obj in gc.get_objects() if isinstance(obj, QueryEngine)]
    memory_context = None
    if engine_instances:
        # Use the most recently created engine
        memory_context = getattr(engine_instances[-1], 'memory_context', None)

    # Save interaction to memory
    memory_manager.add_interaction(
        session_id=session_id,
        query=args.query,
        response=str(response),
        context=memory_context
    )

    print(response)

if __name__ == "__main__":
    main()