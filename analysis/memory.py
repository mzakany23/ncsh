import os
import json
import uuid
from datetime import datetime
from pathlib import Path
import re

class ConversationMemory:
    """Store and manage conversation history for multi-turn interactions."""

    def __init__(self, storage_dir=None, db_path=None):
        """
        Initialize the conversation memory.

        Args:
            storage_dir: Directory to store conversation files (new style)
            db_path: Path to SQLite database (for backward compatibility)
        """
        # Handle backward compatibility - if db_path is provided, convert it to a storage_dir
        if db_path is not None:
            print(f"⚠️ Deprecated: Using db_path ({db_path}) is deprecated, please use storage_dir instead")
            # Convert the db_path to a directory path by using its parent directory
            if isinstance(db_path, str):
                storage_dir = os.path.dirname(db_path)
            else:
                # Assume it's a Path object
                storage_dir = str(db_path.parent)

        self.storage_dir = storage_dir or Path("./conversations")
        self.sessions = {}
        self.ensure_storage_dir()

        # Print info about storage location
        print(f"📝 Conversation memory using directory: {self.storage_dir}")

    def ensure_storage_dir(self):
        """Ensure the storage directory exists."""
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)

    def create_session(self):
        """Create a new session ID."""
        session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.sessions[session_id] = []
        return session_id

    def add_interaction(self, session_id, query, response, context=None):
        """Add an interaction to the session history."""
        if session_id not in self.sessions:
            self.sessions[session_id] = []

        interaction = {
            "timestamp": datetime.now().isoformat(),
            "query": query,
            "response": response,
            "context": context or {}
        }

        self.sessions[session_id].append(interaction)
        self._save_session(session_id)

        # Extract entities from query and response
        self._extract_entities(session_id, query, response, context)

    def _extract_entities(self, session_id, query, response, context):
        """Extract entities like team names from query and response for future context."""
        # Check context first for team info
        extracted_context = {}

        if context:
            # If context already has team/division info, use it
            if 'team' in context:
                extracted_context['last_team'] = context['team']

            if 'division' in context:
                extracted_context['last_division'] = context['division']

            # Store the full query context for multi-turn conversations
            extracted_context['query_context'] = context

        # Update the session with extracted context
        if extracted_context and session_id in self.sessions:
            self.sessions[session_id].append({
                "type": "context",
                "data": extracted_context
            })

    def get_last_team(self, session_id=None):
        """Get the most recently mentioned team."""
        if not session_id or session_id not in self.sessions:
            return None

        for interaction in reversed(self.sessions[session_id]):
            if interaction.get("type") == "context" and "last_team" in interaction.get("data", {}):
                return interaction["data"]["last_team"]

        return None

    def get_last_division(self, session_id=None):
        """Get the most recently mentioned division."""
        if not session_id or session_id not in self.sessions:
            return None

        for interaction in reversed(self.sessions[session_id]):
            if interaction.get("type") == "context" and "last_division" in interaction.get("data", {}):
                return interaction["data"]["last_division"]

        return None

    def get_last_query_context(self, session_id=None):
        """Get the context from the most recent query for multi-turn conversations."""
        if not session_id or session_id not in self.sessions:
            # Try to get the most recent session if none specified
            if self.sessions:
                session_id = sorted(self.sessions.keys())[-1]
            else:
                return None

        for interaction in reversed(self.sessions[session_id]):
            if interaction.get("type") == "context" and "query_context" in interaction.get("data", {}):
                return interaction["data"]["query_context"]

        return None

    def _save_session(self, session_id):
        """Save the session to disk."""
        filename = os.path.join(self.storage_dir, f"{session_id}.json")
        with open(filename, 'w') as f:
            json.dump(self.sessions[session_id], f, indent=2)

    def load_session(self, session_id):
        """Load a session from disk."""
        filename = os.path.join(self.storage_dir, f"{session_id}.json")
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                self.sessions[session_id] = json.load(f)
            return True
        return False

    def format_context(self, session_id):
        """Format the conversation history as context for queries."""
        context = []

        if session_id not in self.sessions:
            if not self.load_session(session_id):
                return ""

        for interaction in self.sessions[session_id]:
            # Only include actual query/response pairs, not context metadata
            if "query" in interaction and "response" in interaction:
                context.append(f"User: {interaction['query']}")
                context.append(f"System: {interaction['response']}")

        return "\n".join(context)

    def get_session_history(self, session_id):
        """
        Get the conversation history for a session in the format expected by the UI.
        Returns a list of (query, response, timestamp) tuples.
        """
        # Make sure the session is loaded
        if session_id not in self.sessions:
            if not self.load_session(session_id):
                return []

        history = []

        # Process the session data to extract query/response pairs
        for item in self.sessions[session_id]:
            # Skip metadata/context entries
            if isinstance(item, dict) and 'type' in item and item['type'] == 'context':
                continue

            # Handle interaction entries
            if isinstance(item, dict) and 'query' in item and 'response' in item:
                query = item.get('query', '')
                response = item.get('response', '')
                timestamp = item.get('timestamp', datetime.now().isoformat())
                history.append((query, response, timestamp))

        return history

    # For backward compatibility
    def get_last_query(self, session_id=None):
        """Get the most recent user query."""
        if not session_id or session_id not in self.sessions:
            return None

        for interaction in reversed(self.sessions[session_id]):
            if isinstance(interaction, dict) and 'query' in interaction:
                return interaction['query']

        return None