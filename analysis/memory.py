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

        # Make sure storage_dir is an absolute path
        if storage_dir:
            if not os.path.isabs(storage_dir):
                # Convert relative path to absolute
                storage_dir = os.path.abspath(storage_dir)
        else:
            # Default to './conversations' but make it absolute
            storage_dir = os.path.abspath("./conversations")

        self.storage_dir = storage_dir
        self.sessions = {}
        self.ensure_storage_dir()

        # Print info about storage location
        print(f"📝 Conversation memory using directory: {self.storage_dir}")

    def ensure_storage_dir(self):
        """Ensure the storage directory exists."""
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)

    def create_session(self, custom_id=None):
        """Create a new session ID."""
        if custom_id:
            session_id = custom_id
        else:
            session_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        self.sessions[session_id] = []
        self._save_session(session_id)
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

        # Extract entities from query and response
        self._extract_entities(session_id, query, response, context)

        # Save session to disk
        self._save_session(session_id)

    def _extract_entities(self, session_id, query, response, context):
        """Extract entities like team names from query and response for future context."""
        # Check context first for team info
        extracted_context = {}

        if context:
            # Debug logging for context
            print(f"📝 Context received in _extract_entities: {context}")

            # If context already has team/division info, use it
            if 'team' in context:
                extracted_context['last_team'] = context['team']
                print(f"📝 Stored team context: {context['team']}")

            if 'division' in context:
                extracted_context['last_division'] = context['division']
                print(f"📝 Stored division context: {context['division']}")

            # Store the full query context for multi-turn conversations
            extracted_context['query_context'] = context

        # If no team in context, try to extract from query using regex
        if 'last_team' not in extracted_context:
            # Enhanced regex to extract team names more generically
            # This is a basic pattern that captures team names followed by FC, United, etc.
            team_pattern = r'(?i)([A-Za-z\s\-\']+\s*(?:FC|United|City|Soccer|Athletic|Club|SC))'
            team_match = re.search(team_pattern, query)

            # If the generic pattern doesn't find a match, try common North Coast teams
            if not team_match:
                nc_teams_pattern = r'(?i)(Key West|Durham|Charlotte|Cleveland|Asheville|Raleigh|Chapel Hill|Wilmington|Greenville|Boone|Outer Banks)'
                team_match = re.search(nc_teams_pattern, query)

            if team_match:
                team_name = team_match.group(1).strip()
                extracted_context['last_team'] = team_name
                print(f"📝 Extracted team from query: {team_name}")

                # Add context about North Coast soccer
                extracted_context['soccer_context'] = "North Coast soccer statistics"
                print(f"📝 Added North Coast soccer context")

        # If no division in context, try to extract from query using regex
        if 'last_division' not in extracted_context:
            # Simple regex to extract division numbers
            division_pattern = r'(?i)division\s*(\d+)|league\s*(\d+)'
            division_match = re.search(division_pattern, query)
            if division_match:
                division = division_match.group(1) or division_match.group(2)
                extracted_context['last_division'] = division
                print(f"📝 Extracted division from query: {division}")

        # Update the session with extracted context
        if extracted_context and session_id in self.sessions:
            context_record = {
                "type": "context",
                "data": extracted_context
            }
            self.sessions[session_id].append(context_record)
            print(f"📝 Added context record to session: {extracted_context}")

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
        """Get the most recent query context for multi-turn conversations."""
        if not session_id or session_id not in self.sessions:
            return None

        for interaction in reversed(self.sessions[session_id]):
            if interaction.get("type") == "context" and "query_context" in interaction.get("data", {}):
                context = interaction["data"]["query_context"]

                # Always ensure North Coast soccer context is included
                if "soccer_context" not in context:
                    context["soccer_context"] = "North Coast soccer statistics"

                return context

        # If no query context found, at least return basic soccer context
        return {"soccer_context": "North Coast soccer statistics"}

    def save_session(self, session_id):
        """Save the session to disk (public method)."""
        return self._save_session(session_id)

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
        """
        Format the conversation history as context for queries.

        Returns a structured representation of the conversation history
        that can be used in prompts for follow-up queries.

        Args:
            session_id: The session ID to get history from

        Returns:
            A formatted string with the conversation history
        """
        if not session_id or session_id not in self.sessions:
            # Try to load the session if it exists
            if session_id and not self.load_session(session_id):
                return ""
            # If still no session, check if we have any sessions
            if not self.sessions:
                return ""
            # Take the first session if none specified
            if not session_id:
                session_id = list(self.sessions.keys())[0]

        # Get session data
        session_data = self.sessions[session_id]

        # Format conversation turns
        conversation = []

        # Keep track of mentions to build a summary
        mentioned_teams = set()
        mentioned_divisions = set()

        for i, interaction in enumerate(session_data):
            # Include query/response pairs
            if interaction.get("type") == "interaction":
                query = interaction.get("query", "")
                response = interaction.get("response", "")

                # Ensure query and response are valid strings
                query = str(query) if query is not None else ""
                response = str(response) if response is not None else "No response available"

                # Add turn number for clarity
                turn_num = i//2 + 1
                conversation.append(f"Turn {turn_num}:")
                conversation.append(f"User: {query}")
                conversation.append(f"System: {response}")
                conversation.append("")  # Empty line for readability

                # Track potential entities mentioned
                team_match = re.search(r'(?i)(Key West FC|FC United|Spartak Cleveland|Cleveland Force FC|Boston Braves FC)', query)
                if team_match:
                    mentioned_teams.add(team_match.group(1))

                division_match = re.search(r'(?i)division\s*(\d+)|league\s*(\d+)', query)
                if division_match:
                    div_num = division_match.group(1) or division_match.group(2)
                    mentioned_divisions.add(div_num)

            # Include context entries for better understanding
            elif interaction.get("type") == "context":
                context_data = interaction.get("data", {})

                # Track mentioned entities from context
                if "last_team" in context_data:
                    team_value = context_data["last_team"]
                    if team_value is not None:
                        mentioned_teams.add(str(team_value))

                if "last_division" in context_data:
                    division_value = context_data["last_division"]
                    if division_value is not None:
                        mentioned_divisions.add(str(division_value))

        # Add a summary of key entities if any were mentioned
        summary = []
        if mentioned_teams:
            # Filter out None values and ensure all are strings
            valid_teams = [str(team) for team in mentioned_teams if team is not None]
            if valid_teams:
                teams_str = ", ".join(valid_teams)
                summary.append(f"Teams mentioned: {teams_str}")

        if mentioned_divisions:
            # Filter out None values and ensure all are strings
            valid_divisions = [str(div) for div in mentioned_divisions if div is not None]
            if valid_divisions:
                divisions_str = ", ".join(valid_divisions)
                summary.append(f"Divisions mentioned: {divisions_str}")

        # Build the final context string
        context_str = ""

        # Add summary at the top if available
        if summary:
            context_str += "CONVERSATION SUMMARY:\n"
            context_str += "\n".join(summary)
            context_str += "\n\n"

        # Add conversation history
        context_str += "CONVERSATION HISTORY:\n"
        context_str += "\n".join(conversation)

        return context_str

    def summarize_context(self, session_id, max_length=1000):
        """
        Create a summarized version of the conversation context.

        If the conversation history is too long, this creates a shorter version
        by keeping the most recent exchanges verbatim and summarizing earlier ones.

        Args:
            session_id: The session ID to summarize
            max_length: Maximum desired length for the summary

        Returns:
            Summarized context string
        """
        full_context = self.format_context(session_id)

        # If context is already within limits, return as is
        if len(full_context) <= max_length:
            return full_context

        # Get session data
        if not session_id or session_id not in self.sessions:
            if not self.load_session(session_id):
                return ""

        session_data = self.sessions[session_id]

        # Keep track of important entities
        teams = set()
        divisions = set()
        time_periods = set()

        # Extract all entities from the conversation
        for interaction in session_data:
            if interaction.get("type") == "context":
                context_data = interaction.get("data", {})
                if "last_team" in context_data and context_data["last_team"] is not None:
                    teams.add(str(context_data["last_team"]))

                if "last_division" in context_data and context_data["last_division"] is not None:
                    divisions.add(str(context_data["last_division"]))

                if "query_context" in context_data:
                    query_ctx = context_data["query_context"]
                    if "team" in query_ctx and query_ctx["team"] is not None:
                        teams.add(str(query_ctx["team"]))

                    if "division" in query_ctx and query_ctx["division"] is not None:
                        divisions.add(str(query_ctx["division"]))

                    if "time_period" in query_ctx and query_ctx["time_period"] is not None:
                        time_periods.add(str(query_ctx["time_period"]))

        # Build a concise summary
        summary = ["CONVERSATION SUMMARY:"]

        if teams:
            teams_str = ", ".join([str(team) for team in teams if team is not None])
            if teams_str:
                summary.append(f"Teams discussed: {teams_str}")

        if divisions:
            divisions_str = ", ".join([str(div) for div in divisions if div is not None])
            if divisions_str:
                summary.append(f"Divisions discussed: {divisions_str}")

        if time_periods:
            periods_str = ", ".join([str(period) for period in time_periods if period is not None])
            if periods_str:
                summary.append(f"Time periods discussed: {periods_str}")

        # Get only recent interactions for verbatim inclusion
        recent_turns = []
        interaction_count = 0

        for interaction in reversed(session_data):
            if interaction.get("type") == "interaction":
                if interaction_count < 4:  # Keep last 2 complete turns (4 interactions)
                    recent_turns.insert(0, interaction)
                    interaction_count += 1
                else:
                    break

        # Format recent turns
        recent_context = []
        for i, interaction in enumerate(recent_turns):
            query = interaction.get("query", "")
            response = interaction.get("response", "")

            # Ensure query and response are valid strings
            query = str(query) if query is not None else ""
            response = str(response) if response is not None else "No response available"

            recent_context.append(f"User: {query}")
            recent_context.append(f"System: {response}")

        # Combine summary and recent interactions
        result = "\n".join(summary)
        result += "\n\nMOST RECENT EXCHANGES:\n"
        result += "\n".join(recent_context)

        return result

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
                # Ensure response is a valid string
                response = item.get('response', '')
                if response is None:
                    response = "No response available"

                timestamp = item.get('timestamp', '')

                # Make sure all values are strings
                query = str(query) if query is not None else ""
                response = str(response) if response is not None else ""
                timestamp = str(timestamp) if timestamp is not None else ""

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

    def list_sessions(self):
        """List all available session files in the storage directory."""
        sessions = []
        if os.path.exists(self.storage_dir):
            for filename in os.listdir(self.storage_dir):
                if filename.endswith('.json'):
                    session_id = filename.replace('.json', '')
                    sessions.append(session_id)
        return sessions