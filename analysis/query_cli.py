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

# Add the parent directory to the path so we can import our modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import from our refactored modules
from analysis.query.core.setup import setup_query_engine, run_query

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
    return parser.parse_args()

def interactive_mode(args):
    """Run the query engine in interactive mode."""
    print("\n🔍 Soccer Query Engine - Interactive Mode 🔍")
    print("Type 'exit', 'quit', or Ctrl+C to exit")
    print("Type 'reset' to reset the conversation memory")
    print("-" * 50)

    # Create a session ID if not provided
    session_id = args.session
    if not session_id:
        now = datetime.now()
        session_id = now.strftime("%Y%m%d_%H%M%S")

    print(f"Session ID: {session_id}")

    # Set up the query engine
    engine = setup_query_engine(
        db_path=args.db,
        model_name=args.model,
        api_key=args.api_key,
        api_base=args.api_base,
        verbose=args.verbose
    )

    # Simple memory implementation
    memory = {"session_id": session_id, "history": []}

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
                memory["history"] = []
                print("Conversation memory reset.")
                continue

            # Process the query
            if query.strip():
                print("\nProcessing query...")
                response = engine.query(query, memory=memory)

                # Store in memory
                memory["history"].append({"query": query, "response": response})

                print("\nResponse:")
                print("-" * 50)
                print(response)
                print("-" * 50)

    except KeyboardInterrupt:
        print("\nExiting...")

def main():
    """Main entry point."""
    args = parse_args()

    # Run in interactive mode if requested or if no query is provided
    if args.interactive or args.query is None:
        interactive_mode(args)
        return

    # Run a single query
    response = run_query(
        query=args.query,
        db_path=args.db,
        model_name=args.model,
        api_key=args.api_key,
        api_base=args.api_base,
        verbose=args.verbose
    )

    print(response)

if __name__ == "__main__":
    main()