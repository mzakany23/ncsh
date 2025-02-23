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

    def get_session_history(self, session_id, limit=5):
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

    def format_context(self, session_id, limit=5):
        """Format the conversation history as context for the next query."""
        history = self.get_session_history(session_id, limit)
        if not history:
            return ""

        context = ["Previous conversation context:"]

        # Track the most recently mentioned team
        last_team = None

        for query, response, stored_context in reversed(history):
            context.append(f"\nQ: {query}")
            context.append(f"A: {response}")

            # Extract team information from context if available
            if stored_context:
                try:
                    ctx = json.loads(stored_context)
                    if ctx.get("matched_team") and ctx["matched_team"] != query:
                        last_team = ctx["matched_team"]
                except json.JSONDecodeError:
                    pass

        # Add the last mentioned team as additional context
        if last_team:
            context.append(f"\nMost recently discussed team: {last_team}")
            context.append("Use this team name for any follow-up questions about 'they' or 'their' performance.")

        return "\n".join(context)