import sqlite3
from datetime import datetime
from pathlib import Path
import json

class ConversationMemory:
    def __init__(self, db_path="conversation_history.db"):
        self.db_path = Path(db_path)
        self._init_db()

    def _init_db(self):
        """Initialize the SQLite database with necessary tables."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id TEXT PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS conversations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT,
                    query TEXT,
                    response TEXT,
                    context JSON,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
                )
            """)

    def create_session(self):
        """Create a new session and return the session ID."""
        session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("INSERT INTO sessions (session_id) VALUES (?)", (session_id,))
        return session_id

    def add_interaction(self, session_id, query, response, context=None):
        """Add a new interaction to the conversation history."""
        with sqlite3.connect(self.db_path) as conn:
            # Update session last_updated timestamp
            conn.execute(
                "UPDATE sessions SET last_updated = CURRENT_TIMESTAMP WHERE session_id = ?",
                (session_id,)
            )
            # Add the interaction
            conn.execute(
                "INSERT INTO conversations (session_id, query, response, context) VALUES (?, ?, ?, ?)",
                (session_id, query, response, json.dumps(context) if context else None)
            )

    def get_session_history(self, session_id, limit=10):
        """Get the conversation history for a session."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT query, response, context
                FROM conversations
                WHERE session_id = ?
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (session_id, limit)
            )
            return cursor.fetchall()

    def format_context(self, session_id, limit=3):
        """Format the conversation history as context for the next query."""
        history = self.get_session_history(session_id, limit)
        if not history:
            return ""

        context_parts = ["Previous conversation context:"]

        # Track the most recently mentioned team
        last_team = None
        last_query_type = None  # Track if we were discussing matches or stats

        for query, response, stored_context in reversed(history):
            # Add query and response
            context_parts.append(f"\nUser: {query}")
            context_parts.append(f"Assistant: {response}")

            # Extract context information
            if stored_context:
                try:
                    ctx = json.loads(stored_context)
                    if ctx.get("matched_team"):
                        last_team = ctx["matched_team"]
                    if ctx.get("query_type"):
                        last_query_type = ctx["query_type"]
                except json.JSONDecodeError:
                    pass

        # Add contextual hints
        if last_team:
            context_parts.append(f"\nMost recently discussed team: {last_team}")
            context_parts.append("Use this team name for follow-up questions about 'they' or 'their' performance.")

        if last_query_type:
            context_parts.append(f"Previous query type: {last_query_type}")
            context_parts.append("Consider this context for interpreting follow-up questions.")

        return "\n".join(context_parts)

    def get_last_team(self, session_id=None):
        """Get the most recently discussed team."""
        if session_id is None:
            # Get the most recent session
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT session_id FROM sessions ORDER BY last_updated DESC LIMIT 1"
                )
                result = cursor.fetchone()
                if not result:
                    return None
                session_id = result[0]

        # Get the most recent interaction with a team context
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT context
                FROM conversations
                WHERE session_id = ?
                  AND context IS NOT NULL
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (session_id,)
            )
            result = cursor.fetchone()
            if not result or not result[0]:
                return None

            try:
                context = json.loads(result[0])
                return context.get("matched_team")
            except json.JSONDecodeError:
                return None

    def set_last_team(self, team_name, session_id=None):
        """Set the most recently discussed team."""
        if session_id is None:
            # Get the most recent session
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT session_id FROM sessions ORDER BY last_updated DESC LIMIT 1"
                )
                result = cursor.fetchone()
                if not result:
                    session_id = self.create_session()
                else:
                    session_id = result[0]

        # Add a context update
        context = {"matched_team": team_name}
        self.add_interaction(
            session_id=session_id,
            query="",  # Empty query since this is just a context update
            response="",  # Empty response since this is just a context update
            context=context
        )