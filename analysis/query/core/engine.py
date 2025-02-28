"""Core query engine for the soccer database.

This module provides the QueryEngine class, which extends the base NLSQLTableQueryEngine
to provide specialized functionality for querying soccer match data.
"""

import re
import json
import logging
from typing import Dict, List, Optional, Tuple, Any, Union
from copy import deepcopy
from datetime import datetime, date, timedelta

from llama_index.core.indices.struct_store import NLSQLTableQueryEngine
from ..utils import (
    is_empty_result,
    has_unrealistic_values,
    get_all_teams
)


# Set up logging
logger = logging.getLogger(__name__)


class TeamMatch:
    """
    Class to represent a team match with confidence score.
    This class is used for fuzzy matching of team names.
    """
    def __init__(self, team_name, confidence, uncertain=False):
        self.team_name = team_name
        self.confidence = confidence
        self.uncertain = uncertain

    def __str__(self):
        return f"{self.team_name}" + (" (uncertain)" if self.uncertain else "")

    def __repr__(self):
        return self.__str__()

    def to_dict(self):
        """Convert the TeamMatch object to a dictionary for JSON serialization."""
        return {
            "team_name": self.team_name,
            "confidence": self.confidence,
            "uncertain": self.uncertain
        }


class QueryEngine(NLSQLTableQueryEngine):
    """Extended query engine for soccer data.

    This class extends the base NLSQLTableQueryEngine to provide specialized
    functionality for querying soccer match data, such as team statistics,
    match histories, etc.
    """

    def __init__(self, sql_database, llm, always_infer=False, **kwargs):
        """
        Initialize the QueryEngine.

        Args:
            sql_database: SQL database to query
            llm: Language model for query generation and response formatting
            always_infer: Always use inference for SQL generation
            **kwargs: Additional arguments to pass to the parent class
        """
        super().__init__(sql_database=sql_database, **kwargs)
        self.sql_database = sql_database
        self.llm = llm
        self.memory_context = None  # Initialize memory context attribute
        self.always_infer = always_infer  # Flag to force using dynamic inference for all queries
        print(f"Query engine initialized with always_infer={self.always_infer}")

        # Enable fuzzy team matching by default
        self.use_fuzzy_team_matching = True

        # Load teams for context
        self.teams = get_all_teams(self.sql_database)
        print(f"📊 Loaded {len(self.teams)} teams")

    def _get_table_context(self) -> str:
        """
        Get the database schema context string for the 'matches' table.

        Returns:
            String representation of the database schema
        """
        return """
        Table: matches
        Columns:
        - date (DATE): The date of the match
        - home_team (TEXT): Name of the home team
        - away_team (TEXT): Name of the away team
        - home_score (INTEGER): Goals scored by home team (NULL if match hasn't been played yet)
        - away_score (INTEGER): Goals scored by away team (NULL if match hasn't been played yet)

        Notes:
        - Database Context:
          * This database contains North Coast soccer match statistics ONLY
          * All team names refer to soccer teams, never to people, places, or other entities
          * All queries should be interpreted in the context of soccer matches and team statistics

        - Division Information:
          * Teams may have division numbers in parentheses, e.g. "Team Name (1)"
          * Division 1 is sometimes referred to as the "C league"
          * Common divisions are 1, 2, and 3
          * Division patterns should be extracted using: REGEXP_EXTRACT(team_name, '\\(([A-Za-z0-9])\\)', 1)

        - NULL Score Handling:
          * Matches with NULL scores are upcoming/future matches that haven't been played yet
          * When calculating win/loss records, only count matches with non-NULL scores
          * For match listing queries, label NULL score matches as "Upcoming" or "Not Played Yet"

        - Current date functions available: CURRENT_DATE

        - Date functions available:
          * DATE_TRUNC('month', date): Truncates to start of month
          * DATE_TRUNC('year', date): Truncates to start of year
          * DATE_TRUNC('week', date): Truncates to start of week
          * INTERVAL arithmetic: date + INTERVAL '1 month'

        Time Period Examples:
        - This month:
          date >= DATE_TRUNC('month', CURRENT_DATE) AND
          date < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'

        - This year:
          date >= DATE_TRUNC('year', CURRENT_DATE) AND
          date < DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1 year'

        - Last 30 days:
          date >= CURRENT_DATE - INTERVAL '30 days'

        - Specific year:
          EXTRACT(year FROM date) = <year>
        """

    def _clean_query(self, query_str: str) -> str:
        """
        Clean the user query to standardize formatting.

        Args:
            query_str: The user query

        Returns:
            Cleaned query string
        """
        # Remove formatting requests
        query_str = re.sub(r'(format|show|display|present)(\s+results)?(\s+as)?\s+(a\s+)?(table|graph|chart|plot|visualization|markdown|json|csv)', '', query_str, flags=re.IGNORECASE)

        # Standardize common phrases
        query_str = re.sub(r'\b(this|current|present)\s+(month|week|year)\b', r'current \2', query_str, flags=re.IGNORECASE)

        # Standardize phrases like "do this month", "did this month" to help with classification
        query_str = re.sub(r'\b(do|did|doing|performed?|results?|stats?)\s+(this|current|present)\s+(month|week|year)\b',
                          r'\1 current \3', query_str, flags=re.IGNORECASE)

        # Standardize "how did [team] do this month" pattern
        query_str = re.sub(r'\bhow\s+did\s+(.*?)\s+do\s+(this|current|present)\s+(month|week|year)\b',
                          r'performance of \1 in current \3', query_str, flags=re.IGNORECASE)

        return query_str.strip()

    def _classify_query(self, query, memory=None):
        """
        Classify the user's query into a specific type.
        """
        # Normalize the query
        query = query.strip().lower()

        print(f"📝 Classifying query: '{query}'")

        # Get context from memory first
        memory_entities = {}
        if memory:
            memory_entities = self._extract_entities_from_memory(memory)
            last_team = memory_entities.get('last_team')
            if last_team:
                print(f"📝 Retrieved team context from memory: {last_team}")

        # Get entity mappings from the query
        entity_mappings = self._extract_entities_from_query(query)

        # If we found entity mappings, print them for debugging
        if entity_mappings:
            print(f"📝 Extracted entity mappings from query: {entity_mappings}")

        # Handle context follow-up
        context_result = self._handle_context_follow_up(query, memory, entity_mappings)
        team_context = context_result.get('team_context')
        follow_up_context = context_result.get('follow_up_context')
        time_filter_context = context_result.get('time_filter_context')

        # Check if we have a last_team in memory but no team_context set
        if 'last_team' in memory_entities and not team_context:
            # If query looks like it might be follow-up but we didn't set context, try using last_team
            follow_up_indicators = ["what", "are", "their", "stats", "month"]
            has_indicators = any(indicator in query.split() for indicator in follow_up_indicators)

            # If the query has follow-up indicators, use the last team from memory
            if has_indicators:
                print(f"📝 Query has follow-up indicators but no team context. Using last_team: {memory_entities['last_team']}")
                team_context = memory_entities['last_team']
                follow_up_context = True

        # Determine query type
        query_type = self._determine_query_type(query, entity_mappings, team_context)

        print(f"📝 Query classified as: {query_type}")
        if team_context:
            print(f"📝 Team context: {team_context}")

        # Create the result context with all relevant information
        result_context = {
            'query': query,
            'query_type': query_type,
            'entity_mappings': entity_mappings,
            'team_context': team_context,
            'follow_up_context': follow_up_context,
            'time_filter_context': time_filter_context
        }

        # Store in memory context for future retrieval
        self.memory_context = result_context

        # Debug
        print(f"📝 Final context result: {result_context}")

        return result_context

    def _handle_context_follow_up(self, query, memory, entity_mappings=None, time_context=None):
        """
        Handle context follow-up by inferring additional context from memory.
        This method determines if the query is likely a follow-up and extracts context accordingly.
        """
        entity_mappings = entity_mappings or {}
        team_context = None
        follow_up_context = None
        time_filter_context = None

        # Extract entities from memory
        entities = self._extract_entities_from_memory(memory)
        last_team = entities.get('last_team')

        print(f"📝 Context follow-up analysis: query='{query}', last_team='{last_team}'")

        # If we have a team context from memory, store it
        if last_team:
            # Check if the query contains pronouns or follow-up indicators
            pronouns = ['they', 'them', 'their', 'it', 'its']
            follow_up_keywords = ['what about', 'how about', 'tell me more', 'continue', 'and']

            # Words that typically indicate we're asking about the team without naming it
            team_keywords = ['record', 'stats', 'statistics', 'standing', 'matches', 'games', 'played', 'win', 'lose', 'lost', 'draw', 'score']

            # Check for pronouns, follow-up keywords, or team keywords
            has_pronoun = any(pronoun in query.lower().split() for pronoun in pronouns)
            has_follow_up_keyword = any(keyword in query.lower() for keyword in follow_up_keywords)
            has_team_keyword = any(keyword in query.lower() for keyword in team_keywords)

            # Check if query likely refers to the team without explicitly mentioning it
            likely_refers_to_team = has_pronoun or has_follow_up_keyword or has_team_keyword

            print(f"📝 Follow-up analysis: has_pronoun={has_pronoun}, has_follow_up_keyword={has_follow_up_keyword}, has_team_keyword={has_team_keyword}")

            # Check for time-specific follow-up indicators
            time_patterns = [
                r'\bthis (month|week|year|season)\b',
                r'\blast (month|week|year|season)\b',
                r'\bnext (month|week|year|season)\b',
                r'\b(january|february|march|april|may|june|july|august|september|october|november|december)\b',
                r'\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b',
                r'\bdo this (month|week|year|season)\b',
                r'\bdid this (month|week|year|season)\b',
                r'\bdoing this (month|week|year|season)\b',
                r'\bperform(ed|ance)? this (month|week|year|season)\b',
                r'\bresults? this (month|week|year|season)\b',
                r'\bstats? this (month|week|year|season)\b'
            ]

            has_time_pattern = any(re.search(pattern, query.lower()) for pattern in time_patterns)

            # For follow-up queries or those with time patterns, we should maintain team context
            if likely_refers_to_team or has_time_pattern:
                print(f"📝 This appears to be a follow-up query, setting team context to: {last_team}")
                team_context = last_team
                follow_up_context = True

                # Set time filter context flag if we detected a time pattern
                if has_time_pattern:
                    time_filter_context = True
                    print(f"📝 Detected time pattern in query, setting time_filter_context=True")

        # Access session ID from memory object if available
        session_id = getattr(memory, 'session_id', None)

        # If we don't have team context from this session but we have a session ID,
        # try to load team context from previous interactions in this session
        if not team_context and session_id:
            team_context = self._load_team_context_from_memory(memory, session_id)
            if team_context:
                follow_up_context = True
                print(f"📝 Loaded team context from memory: {team_context}")

        print(f"📝 Final context determination: team_context={team_context}, follow_up_context={follow_up_context}, time_filter_context={time_filter_context}")

        return {
            'team_context': team_context,
            'follow_up_context': follow_up_context,
            'time_filter_context': time_filter_context
        }

    def _answer_from_context(self, query_str: str, entity_mappings: Dict[str, str], query_context: dict) -> str:
        """
        Generate a response to a query directly from context without SQL.

        Args:
            query_str: The user query
            entity_mappings: Mappings of partial entity names to full names
            query_context: Context about the query

        Returns:
            Natural language response as a string
        """
        print("🧠 Generating response from context without SQL")

        # Create a clean context dictionary without non-serializable objects
        clean_context = {}
        for key, value in query_context.items():
            if key != 'memory' and not hasattr(value, '__dict__'):
                if isinstance(value, (str, int, float, bool, list, dict, type(None))):
                    clean_context[key] = value

        # Extract important context
        team = None

        # First check team_context directly
        if "team_context" in clean_context:
            team = clean_context["team_context"]
            print(f"📝 Using team_context from query_context: {team}")
        elif "team" in clean_context:
            team = clean_context["team"]
            print(f"📝 Using team from query_context: {team}")
        elif "last_team" in clean_context:
            team = clean_context["last_team"]
            print(f"📝 Using last_team from query_context: {team}")

        # If still no team, try finding it in the entity mappings
        if not team:
            for partial_name, full_name in entity_mappings.items():
                if partial_name in query_str.lower():
                    team = full_name
                    print(f"📝 Extracted team from query: {team}")
                    break

        # Track what we found
        print(f"📝 Context received in _extract_entities: {clean_context}")

        # Store the team context for future reference if found
        if team:
            print(f"📝 Stored team context: {team}")

        # Get current date info
        today = date.today()
        current_date_str = today.strftime("%Y-%m-%d")
        current_month_str = today.strftime("%B %Y")

        # Add soccer context to ensure responses stay on topic
        soccer_context = "North Coast soccer statistics"
        if "soccer_context" in clean_context:
            soccer_context = clean_context["soccer_context"]

        print(f"📝 Added North Coast soccer context")

        # Store context for future reference
        context_record = {
            "last_team": team.lower() if team else None,
            "soccer_context": soccer_context
        }

        # Add the query context for more detail if it was passed in
        if clean_context:
            context_record["query_context"] = clean_context

        # Save this context in memory
        # Add to memory context for future reference
        if hasattr(self, 'memory') and self.memory:
            try:
                session_id = getattr(self.memory, 'session_id', None)
                if session_id and hasattr(self.memory, 'sessions'):
                    # Add as a special context record
                    if session_id in self.memory.sessions:
                        self.memory.sessions[session_id].append({
                            "type": "context",
                            "data": context_record
                        })
                        # Save to disk if available
                        if hasattr(self.memory, '_save_session'):
                            self.memory._save_session(session_id)
                        print(f"📝 Added context record to session: {context_record}")
            except Exception as e:
                print(f"⚠️ Error adding context to memory: {str(e)}")

        # Build a prompt for answering from context
        prompt = f"""
        You are answering a user query about North Coast soccer based on the context provided.

        USER QUERY: "{query_str}"

        CONTEXT:
        - Today's date: {current_date_str}
        - Current month: {current_month_str}
        - Context: {soccer_context}
        - Team referred to: {team if team else "No specific team identified"}

        FOLLOW-UP CONTEXT:
        - This query is{"" if clean_context.get("follow_up_context") else " not"} a follow-up query
        - If this is a follow-up query, the team being discussed is: {team if team else "Unknown"}

        IMPORTANT:
        - If the team was identified from the query or context as "{team}", make sure to ONLY talk about this specific team
        - Do not mention other teams in your response unless they were explicitly mentioned in the query
        - Be clear that you're discussing {team} throughout your response
        - If information about {team} is limited, acknowledge that but still focus on this team only

        INSTRUCTIONS:
        - Generate a brief, direct answer to the user's question
        - Only use the information provided in the context
        - Be honest about what you know and don't know
        - If you're unsure, suggest that a database lookup would provide more accurate information
        - Keep your response focused on soccer statistics and team information
        - Always mention the specific team by name when responding about team-specific information

        Your response should be natural and conversational, as if you're answering a question about soccer.
        """

        # Get response from LLM
        response = self.llm.complete(prompt)
        response_text = response.text.strip() if response and hasattr(response, 'text') else "I'm sorry, I couldn't generate a response based on the available context."

        # Build a SQL query suggestion if we have a specific team
        sql_suggestion = None
        if team:
            # Create a simple example query for this team if needed in the future
            sql_suggestion = f"""
            SELECT
                date,
                home_team,
                away_team,
                home_score,
                away_score
            FROM
                matches
            WHERE
                home_team = '{team}' OR away_team = '{team}'
            ORDER BY
                date DESC
            LIMIT 10;
            """

        # Add context about what we identified to the result
        context_info = {
            "response_type": "context_based",
            "identified_team": team,
            "sql_suggestion": sql_suggestion
        }

        return response_text

    def _infer_response(self, query_str: str, results, query_context: dict) -> str:
        """
        Generate a natural language response from the SQL results.
        """
        # Extract context
        entity_mappings = query_context.get("entity_mappings", {})
        team_context = query_context.get("team_context")
        follow_up_context = query_context.get("follow_up_context", False)
        time_filter_context = query_context.get("time_filter_context", False)
        generated_sql = query_context.get("generated_sql", "")

        # Check for uncertain team match
        uncertain_team_match = query_context.get("uncertain_team_match", False)
        uncertain_team = query_context.get("uncertain_team", None)

        # Convert results to JSON string for LLM
        results_str = "No results found."
        if results is not None:
            if hasattr(results, "to_json"):
                try:
                    results_str = results.to_json(orient="records")
                except Exception as e:
                    results_str = str(results)
            else:
                results_str = str(results)

        # Count the number of results
        num_results = 0
        if hasattr(results, "__len__"):
            num_results = len(results)

        # Prepare base system message
        system_message = f"""You are an expert soccer data analyst providing insights in response to user questions about soccer matches.

USER QUERY: "{query_str}"

SQL QUERY USED:
{generated_sql}

RESULT SET:
{results_str}

Number of results: {num_results}

CONTEXT:
- This is a soccer database for the North Coast Soccer League
- You should provide a comprehensive but concise natural language answer to the user's query
- Reference specific data from the result set when applicable
- Always interpret team names as soccer teams, never as people or other entities"""

        # Add team context if available
        if team_context:
            system_message += f"""
- The user is specifically asking about the team: {team_context}
- Make sure to center your response around this team's data"""

        # Add uncertain match context if applicable
        if uncertain_team_match and uncertain_team:
            system_message += f"""
IMPORTANT NOTE ABOUT TEAM MATCHING:
- The system detected a potential reference to the team "{uncertain_team}" in the query: "{query_str}"
- This match is uncertain, so we searched broadly using the pattern '%{uncertain_team}%'
- If this is incorrect, include a note in your response asking the user to specify the team name more precisely"""

        # Give guidance based on results
        system_message += """

FORMAT YOUR RESPONSE:
1. First, provide a direct answer to the question using data from the result set
2. Include relevant statistics, win/loss records, or match information depending on the query
3. If no results were found, explain why there might be no data and suggest alternative queries
4. For team-specific queries with no results, confirm if this is the team they're looking for and suggest similar team names
5. Keep your response friendly and conversational
6. Do not mention the SQL query or that you're querying a database

Your response should sound like a knowledgeable soccer analyst, not a database query result."""

        # Generate the response with the LLM
        try:
            # For empty results, provide a more helpful message
            if num_results == 0:
                # Special handling for no results but uncertain team match
                if uncertain_team_match and uncertain_team:
                    team_options = self._find_similar_teams(uncertain_team)
                    team_suggestions = ", ".join([f'"{team}"' for team in team_options[:5]])

                    no_results_prompt = f"""You need to inform the user that no matches were found for "{uncertain_team}".

The system is uncertain if "{uncertain_team}" is the correct team name the user is looking for.

Here are some similar team names in our database: {team_suggestions}

Please generate a helpful response that:
1. Informs the user no matches were found for their query
2. Suggests they might have meant one of the similar teams listed
3. Asks them to clarify which team they're interested in
4. Provides suggestions for better search terms"""

                    response = self.llm.complete(no_results_prompt)
                    # Extract content from response object
                    if hasattr(response, 'content'):
                        return response.content
                    elif hasattr(response, 'text'):
                        return response.text
                    else:
                        return str(response)

                # Standard no results handling
                no_results_prompt = f"""You need to inform the user that no results were found for their query: "{query_str}".

Generate a helpful response that:
1. Clearly states that no matches or data were found
2. Suggests possible reasons why (e.g., no matches in that date range, team name misspelled, etc.)
3. Recommends alternative queries they might try
4. Maintains a helpful and constructive tone"""

                response = self.llm.complete(no_results_prompt)
                # Extract content from response object
                if hasattr(response, 'content'):
                    return response.content
                elif hasattr(response, 'text'):
                    return response.text
                else:
                    return str(response)

            # Standard response for results
            response = self.llm.complete(system_message)
            # Extract content from response object
            if hasattr(response, 'content'):
                return response.content
            elif hasattr(response, 'text'):
                return response.text
            else:
                return str(response)

        except Exception as e:
            print(f"❌ Error generating response: {str(e)}")
            return f"I'm sorry, I encountered an error while generating a response: {str(e)}"

    def _find_similar_teams(self, team_name):
        """Find teams with similar names to help users when uncertain matches occur."""
        similar_teams = []

        # Convert to lowercase for matching
        team_name_lower = team_name.lower()

        # Try with spaces for cases like "KeyWest" -> "Key West"
        # Insert spaces between capital letters: "KeyWest" -> "Key West"
        spaced_team_name = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', team_name).lower()

        for team in self.teams:
            team_lower = team.lower()

            # Check for partial match
            if team_name_lower in team_lower or team_lower in team_name_lower:
                similar_teams.append(team)
                continue

            # Check for match with spaced version
            if spaced_team_name in team_lower or team_lower in spaced_team_name:
                similar_teams.append(team)
                continue

            # Check for similarity using longest common substring
            s1, s2 = team_name_lower, team_lower
            if len(s1) > len(s2):
                s1, s2 = s2, s1

            match_length = 0
            for i in range(len(s1)):
                if s1[i:] in s2:
                    match_length = len(s1) - i
                    break

            similarity = match_length / len(s1) if len(s1) > 0 else 0
            if similarity > 0.5:
                similar_teams.append(team)

        return similar_teams

    def _handle_ambiguous_query(self, query_str: str, error_message: str = None) -> str:
        """
        Handle ambiguous or failed queries by providing helpful guidance to the user.

        Instead of showing technical errors, this method creates a user-friendly response
        that asks for clarification and provides examples of well-formed questions.

        Args:
            query_str: The original user query
            error_message: Optional error message that was generated

        Returns:
            A user-friendly response with guidance
        """
        print(f"🤔 Handling ambiguous query: {query_str}")

        # Extract potential entities from the query to provide more relevant examples
        potential_entities = self._extract_entities_from_query(query_str)

        # Check if the query contains any specific words that might indicate intent
        query_lower = query_str.lower()
        intent_categories = {
            "team info": ["who", "what team", "about team"],
            "matches": ["match", "game", "played", "against"],
            "performance": ["score", "win", "loss", "record", "statistics", "stats", "performance", "how did"],
            "time-based": ["month", "week", "year", "season", "recent"]
        }

        detected_intents = []
        for intent, keywords in intent_categories.items():
            if any(keyword in query_lower for keyword in keywords):
                detected_intents.append(intent)

        # Prepare example questions based on detected intents
        example_questions = []

        # Get a sample team name to use in examples
        sample_teams = ["Key West FC", "Sleigh All Day", "The Dude Abides", "Dawgs Out FC", "All In One"]
        sample_team = sample_teams[0]  # Default to first team

        # Look for potential team in the query
        for entity, mapping in potential_entities.items():
            if mapping:
                sample_team = mapping
                break

        # Generate intent-specific examples
        if "team info" in detected_intents:
            example_questions.append(f"Tell me about {sample_team}")
            example_questions.append(f"Who is {sample_team}?")
            example_questions.append(f"What division is {sample_team} in?")

        if "matches" in detected_intents:
            example_questions.append(f"Show me all matches played by {sample_team}")
            example_questions.append(f"When did {sample_team} last play against The Crew?")
            example_questions.append(f"List all home matches for {sample_team}")

        if "performance" in detected_intents:
            example_questions.append(f"How did {sample_team} perform this month?")
            example_questions.append(f"What is {sample_team}'s win-loss record?")
            example_questions.append(f"Show me {sample_team}'s scoring statistics")

        if "time-based" in detected_intents:
            example_questions.append(f"How did {sample_team} do this month?")
            example_questions.append(f"Show me matches from last season")
            example_questions.append(f"What was {sample_team}'s performance in February 2025?")

        # If no specific intent was detected or we still need more examples
        if not example_questions or len(example_questions) < 3:
            # Add some general examples
            example_questions.extend([
                f"Show me all matches where {sample_team} scored more than 3 goals",
                f"What is {sample_team}'s win percentage in away games?",
                "Show me the top 5 teams by goals scored",
                "List all matches played in March 2025",
                "Compare home vs away performance for Key West FC"
            ])

        # Limit to 5 examples
        example_questions = example_questions[:5]

        # Create a prompt for our LLM to generate a helpful response
        prompt = f"""
        Your goal is to help the user get better results from their query about "{query_str}" which was either ambiguous
        or couldn't be processed properly. You should:

        1. Acknowledge their query in a friendly way
        2. Provide guidance on how to formulate more specific questions about North Coast soccer data
        3. Suggest 3-5 specific example questions they could ask instead
        4. Keep your tone conversational and helpful

        Additional context:
        - This is about North Coast soccer statistics in a database
        - User's query: "{query_str}"
        - Detected possible intents: {", ".join(detected_intents) if detected_intents else "unclear"}
        - Potential entities: {potential_entities}
        - Example questions to suggest: {example_questions}

        Do not mention SQL, technical errors, or coding issues. Focus on helping the user ask better questions about soccer data.
        """

        # Get response from LLM
        try:
            response = self.llm.complete(prompt)

            # Get the text from the response
            if hasattr(response, 'content'):
                return response.content
            elif hasattr(response, 'text'):
                return response.text
            else:
                return str(response)
        except Exception as e:
            print(f"Error in _handle_ambiguous_query: {str(e)}")
            return f"I'm not sure I understand your question about '{query_str}'. Could you please provide more details about which soccer team or match information you're looking for?"

    def _execute_sql(self, sql: str):
        """
        Execute SQL query with error handling.

        Args:
            sql: SQL query to execute

        Returns:
            Query results

        Raises:
            Exception: If there's an error executing the SQL
        """
        logger.info(f"🔍 Executing SQL: {sql}")

        try:
            results = self.sql_database.run_sql(sql)
            return results
        except Exception as e:
            error_message = str(e)
            logger.error(f"❌ Error executing SQL query: {error_message}")
            # Re-raise the exception to be caught by the caller
            raise

    def _infer_query_and_generate_sql(self, query_str: str, query_context: dict) -> tuple:
        """
        Infer SQL from natural language query using LLM.

        Returns:
            tuple: (sql_str, updated_context)
        """
        # Check for entity mappings that can be used for the SQL
        entity_mappings = query_context.get("entity_mappings", {})

        # Get team context information
        team_context = query_context.get("team_context")
        follow_up_context = query_context.get("follow_up_context", False)
        time_filter_context = query_context.get("time_filter_context", False)

        # Check for uncertain team matches
        uncertain_team_match = False
        uncertain_team = None

        # If we have entity mappings, use them to set the team context
        if entity_mappings:
            # Get the first team from entity mappings if team_context is not set
            if not team_context:
                for entity, mapping in entity_mappings.items():
                    # For string mappings
                    if isinstance(mapping, str):
                        team_context = mapping
                        break
                    # For TeamMatch objects
                    elif hasattr(mapping, 'team_name'):
                        team_context = mapping.team_name
                        break

            for entity, mapping in entity_mappings.items():
                # Check if the entity confidence indicates uncertainty
                if hasattr(mapping, 'uncertain') and mapping.uncertain:
                    uncertain_team_match = True
                    uncertain_team = entity
                    break
                # For string mappings (backward compatibility)
                elif isinstance(mapping, str) and entity in query_str.lower():
                    # Already set team_context above
                    pass

        # Add uncertainty flags to context
        query_context["uncertain_team_match"] = uncertain_team_match
        if uncertain_team_match and uncertain_team:
            query_context["uncertain_team"] = uncertain_team

        # Prepare system prompt
        system_prompt = f"""You are an assistant that translates natural language queries about soccer matches into SQL.

USER QUERY: "{query_str}"

DATABASE SCHEMA:
- Table: matches
- Columns:
  - date: date of the match (YYYY-MM-DD)
  - home_team: name of the home team
  - away_team: name of the away team
  - home_score: goals scored by home team (NULL if match hasn't been played)
  - away_score: goals scored by away team (NULL if match hasn't been played)
  - league: league/division name
  - time: time of the match
  - url: URL to match details
  - type: type of match (e.g., regular season, tournament)
  - status: match status code
  - headers: additional match information

{f"TEAM CONTEXT: The query is specifically about the team '{team_context}'" if team_context else ""}
{f"FOLLOW-UP CONTEXT: This is a follow-up query that continues the conversation about previously mentioned teams or match details." if follow_up_context else ""}
{f"TIME FILTER CONTEXT: The query involves filtering by time periods like months, seasons, or specific date ranges." if time_filter_context else ""}
"""

        # Add specific instructions for uncertain team matches
        if uncertain_team_match and uncertain_team:
            system_prompt += f"""
IMPORTANT - UNCERTAIN TEAM MATCH:
The user mentioned "{uncertain_team}" which doesn't exactly match any team name.
When generating SQL, use LIKE patterns with wildcards on both sides:
  WHERE home_team LIKE '%{uncertain_team}%' OR away_team LIKE '%{uncertain_team}%'
This will help catch fuzzy matches for "{uncertain_team}".
"""
        # Add specific instructions for the team name when we have a mapped team
        elif team_context:
            system_prompt += f"""
IMPORTANT - TEAM NAME:
Use the exact team name '{team_context}' in the SQL query:
  WHERE home_team = '{team_context}' OR away_team = '{team_context}'
"""

        # Add conversation history context if available
        if query_context.get("conversation_history"):
            system_prompt += f"\nCONVERSATION HISTORY:\n{query_context['conversation_history']}\n"

        # Add additional query context
        today = date.today()
        current_date_str = today.strftime("%Y-%m-%d")
        current_year = today.year
        current_month = today.strftime('%B')

        system_prompt += f"""
ADDITIONAL CONTEXT:
- Today's date is {current_date_str}
- Current year is {current_year}
- Current month is {current_month}
- Matches with NULL scores that are in the future haven't been played yet
- When filtering for time periods, use appropriate date functions/formats
- Ensure proper handling of NULL values in the score columns

INSTRUCTIONS:
1. Generate valid SQL that answers the user's query
2. Use proper SQL syntax for the database schema provided
3. Include appropriate WHERE clauses that map to the user's intent
4. If ordering results, include ORDER BY clauses in a logical way (e.g., by date, score)
5. If the query implies a limit on results, include a LIMIT clause
6. Ensure proper handling of NULL values (e.g., for matches not yet played)
7. Output ONLY the SQL query, with no additional text or explanation
"""

        try:
            print(f"🔍 Generating SQL for query: {query_str}")
            # Generate SQL with the LLM
            response = self.llm.complete(system_prompt)

            # Get the SQL from the response
            # For compatibility with different LLM response formats
            if hasattr(response, 'content'):
                sql = response.content
            elif hasattr(response, 'text'):
                sql = response.text
            else:
                sql = str(response)

            # Clean up the SQL
            sql = sql.strip()

            # Remove any markdown code block formatting
            sql = re.sub(r'^```sql\s*', '', sql)
            sql = re.sub(r'^```\s*', '', sql)
            sql = re.sub(r'\s*```$', '', sql)

            # Update context with generated SQL for response generation
            query_context["generated_sql"] = sql

            return sql, query_context
        except Exception as e:
            error_msg = f"Error generating SQL: {str(e)}"
            print(f"❌ {error_msg}")
            raise ValueError(error_msg)

    def query(self, query_str: str, memory=None) -> str:
        """
        Process a natural language query and return a response.

        Args:
            query_str: The natural language query
            memory: Optional memory object for conversation context

        Returns:
            Natural language response
        """
        logger.info(f"📝 Using memory session: {self._get_or_create_session_id(memory)}")

        # Clean the query string - standardize formatting, etc.
        query_str = self._clean_query(query_str)

        # Initialize context with query
        query_context = {"query": query_str}

        # Try to extract additional context (like previously mentioned teams)
        memory_context = self._load_additional_context(memory)
        if memory_context:
            logger.info(f"📝 Retrieved additional context from memory: {memory_context}")
            query_context.update(memory_context)

        # Extract session ID for memory operations
        session_id = getattr(memory, 'session_id', None) if memory else None

        try:
            # Validate if the query is specific enough for SQL generation
            is_valid, reason = self._validate_query(query_str, query_context)

            # If the query isn't specific enough and doesn't have helpful context,
            # use the ambiguity handler to provide guidance
            if not is_valid:
                logger.info(f"Query validation failed: {reason}")
                return self._handle_ambiguous_query(query_str, f"Query lacks specificity: {reason}")

            # Classify the query and extract relevant entities
            query_info = self._classify_query(query_str, memory)

            # Update context with classification results
            if query_info:
                entity_mappings = query_info.get("entity_mappings", {})

                # If we have entity mappings, we can extract more specific information
                if entity_mappings:
                    print(f"📝 Extracted entity mappings from query: {entity_mappings}")

                # Check for follow-up contexts
                team_context = query_info.get("team_context")
                time_filter_context = query_info.get("time_filter_context")

                # Update query context with the extracted information
                query_context.update(query_info)

                # Process the query based on its type
                if query_info["query_type"] == "follow_up":
                    # Execute follow-up query handling
                    results = self._handle_context_follow_up(query_info)
                    response = self._infer_response(query_str, results, query_context)
                elif query_info["query_type"] == "context_answerable":
                    # Check if this is a context_answerable query that lacks sufficient context
                    if (not entity_mappings or len(entity_mappings) < 1 or
                        (not query_info.get("team_context") and not query_info.get("follow_up_context"))
                    ):
                        logger.info(f"📝 Detected ambiguous context_answerable query without sufficient context")
                        response = self._handle_ambiguous_query(query_str)
                    else:
                        # Answer directly from context
                        response = self._answer_from_context(query_str, entity_mappings, query_context)
                else:  # "new_query"
                    # Create SQL query
                    logger.info(f"📝 Generating SQL for query: {query_str}")
                    sql, context_update = self._infer_query_and_generate_sql(query_str, query_context)
                    query_context.update(context_update)

                    # Execute SQL and get results
                    try:
                        results = self._execute_sql(sql)

                        # Generate response from results
                        response = self._infer_response(query_str, results, query_context)
                    except Exception as e:
                        error_message = str(e)
                        logger.error(f"❌ Error executing SQL query: {error_message}")
                        # Instead of returning the error, use the ambiguous query handler
                        response = self._handle_ambiguous_query(query_str, error_message)
        except Exception as e:
            error_message = str(e)
            logger.error(f"❌ Error processing query: {error_message}")
            # Use ambiguous query handler for all types of errors
            response = self._handle_ambiguous_query(query_str, error_message)

        # Save interaction to memory for context tracking
        if memory and session_id:
            self._save_interaction_to_memory(memory, session_id, query_str, response, query_context)

        # Save the current context to the object itself for optional retrieval
        self.memory_context = query_context.copy() if query_context else {}

        return response

    def _get_or_create_session_id(self, memory):
        """Get the existing session ID or create a new one."""
        session_id = getattr(memory, 'session_id', None)
        if not session_id:
            # Get the session ID from memory if it's available
            if hasattr(memory, 'sessions') and memory.sessions:
                # Take the most recent session if multiple exist
                session_id = list(memory.sessions.keys())[0]

        # Store the session ID for later reference
        memory.session_id = session_id
        return session_id

    def _load_team_context_from_memory(self, memory, session_id):
        """
        Load team context from memory using the session ID.
        """
        # Check if the memory object contains session information
        if not hasattr(memory, 'sessions') or not session_id:
            return None

        # Try to find a team reference in the conversation history
        return self._find_team_in_conversation_history(memory, session_id)

    def _find_team_in_conversation_history(self, memory, session_id):
        """
        Search through conversation history to find the most recent team reference.
        """
        # Get the session data if it exists
        if not hasattr(memory, 'sessions') or not session_id or not memory.sessions:
            return None

        session_data = memory.sessions.get(session_id, [])

        # First check for explicit "last_team" entries in context data
        for interaction in reversed(session_data):
            if isinstance(interaction, dict) and 'type' in interaction and interaction['type'] == 'context':
                context_data = interaction.get('data', {})
                if 'last_team' in context_data:
                    print(f"📝 Found last_team in context data: {context_data['last_team']}")
                    return context_data['last_team']

        # Now check for team context in regular interaction context
        for interaction in reversed(session_data):
            # Check if we have context information
            if isinstance(interaction, dict) and 'context' in interaction:
                context = interaction.get('context', {})
                if isinstance(context, dict):
                    # Check for team information in various fields
                    if 'team' in context:
                        print(f"📝 Found team in interaction context: {context['team']}")
                        return context['team']
                    elif 'last_team' in context:
                        print(f"📝 Found last_team in interaction context: {context['last_team']}")
                        return context['last_team']
                    elif 'team_context' in context:
                        print(f"📝 Found team_context in interaction context: {context['team_context']}")
                        return context['team_context']

                    # Check entity mappings for team names
                    elif 'entity_mappings' in context and context['entity_mappings']:
                        entity_mappings = context['entity_mappings']
                        # Check common keywords for teams
                        for key in ['key west', 'key', 'west', 'team']:
                            if key in entity_mappings:
                                print(f"📝 Found team in entity mappings: {entity_mappings[key]}")
                                return entity_mappings[key]

                        # If no specific keywords found, return the first entity mapping
                        # as it's likely to be the team mentioned
                        if entity_mappings:
                            first_entity = next(iter(entity_mappings.values()))
                            print(f"📝 Using first entity mapping as team: {first_entity}")
                            return first_entity

        return None

    def _save_interaction_to_memory(self, memory, session_id, query, response, context):
        """Save the current interaction to memory."""
        try:
            # Create a serializable copy of the context
            serializable_context = {}
            for key, value in context.items():
                # Skip non-serializable objects like 'memory'
                if key != 'memory' and not hasattr(value, '__dict__'):
                    # Only include serializable types
                    if isinstance(value, (str, int, float, bool, list, dict, type(None))):
                        serializable_context[key] = value

            # Explicitly store team context for easier retrieval
            if "team" in context:
                serializable_context["last_team"] = context["team"]
            elif "team_context" in context:
                serializable_context["last_team"] = context["team_context"]

            memory.add_interaction(
                session_id=session_id,
                query=query,
                response=response,
                context=serializable_context
            )
        except Exception as e:
            print(f"⚠️ Error saving to memory: {str(e)}")
            # If the error is related to serialization, try with minimal context
            try:
                minimal_context = {"last_team": context.get("team", context.get("team_context"))}
                memory.add_interaction(
                    session_id=session_id,
                    query=query,
                    response=response,
                    context=minimal_context
                )
                print(f"📝 Saved with minimal context after error")
            except Exception as e2:
                print(f"⚠️ Error saving with minimal context: {str(e2)}")

    def reset_memory(self):
        """Reset the conversation memory."""
        self.memory_context = None

    def _determine_query_type(self, query, entity_mappings, team_context):
        """
        Determine the query type based on the query content and context.
        """
        query_lower = query.lower()

        # Check for time-based queries (highest priority)
        if any(pattern in query_lower for pattern in ['this month', 'last month', 'this year', 'next week', 'current month']):
            return "new_query"

        # Check for performance/result queries (high priority)
        if any(pattern in query_lower for pattern in ['how did', 'performance', 'do this', 'result', 'score', 'win', 'lose', 'draw']):
            return "new_query"

        # Check for statistical queries that would need SQL (high priority)
        if any(pattern in query_lower for pattern in ['stats', 'statistics', 'record', 'performance', 'ranking']):
            return "new_query"

        # Check for match listing queries (should be SQL)
        if any(pattern in query_lower for pattern in ['show me matches', 'list matches', 'show matches', 'all matches', 'games for', 'matches for']):
            return "new_query"

        # If we have team context from a previous query, this is likely a follow-up
        if team_context:
            return "follow_up"

        # If we have entity mappings and none of the above patterns match, this is likely a context-answerable query
        if entity_mappings:
            # But if we're asking about matches for a specific team, that should be a new query
            if any(pattern in query_lower for pattern in ['matches', 'games', 'played', 'fixtures']):
                logger.info(f"Query is about matches with entity mappings - classified as new_query")
                return "new_query"
            return "context_answerable"

        # Check for informational queries about a team or soccer in general
        if any(pattern in query_lower for pattern in ['who is', 'what is', 'tell me about']):
            return "context_answerable"

        # Default to new query if we can't determine
        return "new_query"

    def _extract_entities_from_query(self, query_str: str, context=None) -> Dict[str, object]:
        """
        Extract entities (like team names) from the query.

        Returns a dictionary mapping entity tokens to their canonical forms.
        """
        print(f"📝 Extracted entity mappings from query: {self._extract_team_names(query_str)}")

        # Check if we should use the updated fuzzy matching system
        if hasattr(self, 'use_fuzzy_team_matching') and self.use_fuzzy_team_matching:
            return self._extract_team_names_fuzzy(query_str.lower())
        else:
            return self._extract_team_names(query_str)

    def _extract_team_names(self, query_str: str) -> Dict[str, str]:
        """
        Extract team names using basic pattern matching.

        Args:
            query_str: The user query string

        Returns:
            Dictionary mapping team name tokens to their canonical forms
        """
        entity_mappings = {}
        query_lower = query_str.lower()

        # First try exact matches
        for team in self.teams:
            team_lower = team.lower()
            # Check if team name is in the query (basic substring match)
            if team_lower in query_lower:
                # Use the team name without spaces as the key (e.g., "keywest" -> "Key West")
                entity_mappings[team_lower.replace(" ", "")] = team

        # If no exact matches, try partial matches (e.g., "Key West" -> "Key West FC")
        if not entity_mappings:
            for team in self.teams:
                team_lower = team.lower()
                team_parts = team_lower.split()

                # Try to match the base name without suffixes like "FC", "United", etc.
                base_name = " ".join(part for part in team_parts if part not in ["fc", "united", "(1)", "(2)", "(3)", "sc"])
                if base_name and base_name in query_lower:
                    # Use the base name without spaces as the key
                    entity_mappings[base_name.replace(" ", "")] = team

                # For teams with multiple words, try matching just the first two words
                if len(team_parts) > 2 and " ".join(team_parts[:2]) in query_lower:
                    entity_mappings[" ".join(team_parts[:2]).replace(" ", "")] = team

        return entity_mappings

    def _extract_team_names_fuzzy(self, query: str) -> Dict[str, object]:
        """
        Extract team names using fuzzy matching. Returns both exact and partial matches with confidence scores.
        """
        entity_mappings = {}
        uncertain_matches = {}  # Dictionary to store matches that are not certain

        # Break the query into lowercase words and filter out common words
        words = re.findall(r'\b\w+\b', query.lower())
        common_words = {'the', 'team', 'club', 'soccer', 'football', 'fc', 'sc', 'united', 'show', 'me', 'all', 'matches', 'games', 'played', 'by', 'against', 'with', 'vs', 'versus', 'and', 'or', 'in', 'for', 'how', 'did', 'do', 'perform', 'performance', 'this', 'last', 'next', 'year', 'month', 'week', 'tomorrow', 'yesterday', 'today', 'season', 'recent'}
        filtered_words = [word for word in words if word not in common_words and len(word) > 2]

        # First, check for exact matches in team names
        for team in self.teams:
            team_lower = team.lower()
            if team_lower in query:
                # Create a TeamMatch object with high confidence score
                # Use the team name without spaces as the key (e.g., "keywest" -> "Key West")
                key = team_lower.replace(" ", "")
                entity_mappings[key] = TeamMatch(team, 0.95, False)
                return entity_mappings

        # Special check for combined words like "KeyWest" that should match "Key West"
        combined_word_matches = {}
        for potential_combined in words:
            if len(potential_combined) > 4:  # Only check longer words
                # Insert spaces between lowercase and uppercase letters
                spaced_word = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', potential_combined).lower()

                # Only if it actually has a space when separated
                if spaced_word != potential_combined.lower() and len(spaced_word.split()) > 1:
                    best_match = None
                    best_score = 0.0

                    for team in self.teams:
                        team_lower = team.lower()
                        # If the spaced word is in the team name
                        if spaced_word in team_lower:
                            score = 0.8 + (len(spaced_word) / len(team_lower))
                            if score > best_score:
                                best_score = score
                                best_match = team
                        # Also check if the words appear in sequence
                        elif all(part in team_lower for part in spaced_word.split()):
                            score = 0.7
                            if score > best_score:
                                best_score = score
                                best_match = team

                    if best_match and best_score > 0.7:
                        combined_word_matches[potential_combined] = (best_match, best_score)

        # If we found any combined word matches, use the best one
        if combined_word_matches:
            best_combined = max(combined_word_matches.items(), key=lambda x: x[1][1])
            combined_word, (team_name, score) = best_combined
            entity_mappings[combined_word.lower()] = TeamMatch(team_name, score, False)
            return entity_mappings

        # If no exact matches, try partial matches
        for word in filtered_words:
            best_match = None
            best_score = 0.0

            for team in self.teams:
                team_lower = team.lower()
                team_parts = team_lower.split()

                # First, check if the word is simply part of the team name
                if word in team_lower:
                    # Calculate how much of the word is part of the team name
                    # by creating a similarity score between 0-1
                    score = len(word) / len(team_lower)
                    if score > best_score:
                        best_score = score
                        best_match = team

                # Next, check for similarity using team name parts
                for team_part in team_parts:
                    # Simple similarity calculation based on longest common substring
                    common_length = 0
                    for i in range(min(len(word), len(team_part))):
                        if word[i] == team_part[i]:
                            common_length += 1
                        else:
                            break

                    if common_length > 2:  # Only consider if at least 3 characters match
                        score = common_length / max(len(word), len(team_part))
                        if score > best_score:
                            best_score = score
                            best_match = team

                # Try pattern like "KeyWest" which should match "Key West"
                if len(word) > 4:  # Only try for longer words
                    # Insert spaces between lowercase and uppercase letters
                    spaced_word = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', word).lower()

                    # If the word with added spaces is in the team name
                    if spaced_word != word and spaced_word in team_lower:
                        score = 0.6 + (len(spaced_word) / len(team_lower))
                        if score > best_score:
                            best_score = score
                            best_match = team

                    # Try joining words in team name
                    if len(team_parts) > 1:
                        joined_team = ''.join(team_parts)
                        if word in joined_team:
                            score = 0.6 + (len(word) / len(joined_team))
                            if score > best_score:
                                best_score = score
                                best_match = team

            # If we found a reasonable match
            if best_match and best_score > 0.3:
                # Set uncertain flag if the confidence is below threshold
                uncertain = best_score < 0.7

                # Add team to mappings with confidence score
                if uncertain:
                    uncertain_matches[word] = TeamMatch(best_match, best_score, uncertain)
                else:
                    entity_mappings[word] = TeamMatch(best_match, best_score, uncertain)

        # Special handling for combined tokens like "KeyWest" that should match "Key West"
        # This is a more general approach in case the first combined word check didn't find matches
        if not entity_mappings:
            for word in words:
                if len(word) > 5 and word not in entity_mappings:  # Only try for longer words
                    # Insert spaces between lowercase and uppercase letters
                    spaced_word = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', word).lower()

                    if spaced_word != word.lower():  # Only if it actually has a space when separated
                        best_match = None
                        best_score = 0.0

                        for team in self.teams:
                            team_lower = team.lower()
                            # Calculate similarity between the spaced word and team name
                            if spaced_word in team_lower:
                                score = 0.7 + (len(spaced_word) / len(team_lower))
                                if score > best_score:
                                    best_score = score
                                    best_match = team

                            # Also try with team name parts joined
                            team_parts = team_lower.split()
                            if len(team_parts) > 1:
                                joined_parts = ''.join(team_parts)
                                if word.lower() in joined_parts:
                                    score = 0.7
                                    if score > best_score:
                                        best_score = score
                                        best_match = team

                        if best_match and best_score > 0.4:
                            uncertain = best_score < 0.7
                            if uncertain:
                                uncertain_matches[word] = TeamMatch(best_match, best_score, uncertain)
                            else:
                                entity_mappings[word] = TeamMatch(best_match, best_score, uncertain)

        # If we didn't find any certain matches but we have uncertain matches,
        # return the best uncertain match
        if not entity_mappings and uncertain_matches:
            # Get the match with the highest confidence score
            best_match_word = max(uncertain_matches.items(), key=lambda x: x[1].confidence)[0]
            entity_mappings[best_match_word] = uncertain_matches[best_match_word]

        return entity_mappings

    def _extract_entities_from_memory(self, memory):
        """
        Extract entities from memory (like previously mentioned teams).
        """
        entities = {}

        # If no memory is provided, return empty entities
        if not memory:
            return entities

        # Try to extract the last team mentioned from the session data
        if hasattr(memory, 'sessions') and hasattr(memory, 'session_id'):
            session_id = memory.session_id
            print(f"📝 Trying to extract entities from session: {session_id}")

            # Use our improved method to find team context
            last_team = self._find_team_in_conversation_history(memory, session_id)
            if last_team:
                entities['last_team'] = last_team
                print(f"📝 Found last team in session history: {last_team}")
                return entities

        # Try to extract the last team mentioned if memory has that capability
        if hasattr(memory, 'get_last_entity') and callable(memory.get_last_entity):
            try:
                last_team = memory.get_last_entity('team')
                if last_team:
                    entities['last_team'] = last_team
                    print(f"📝 Found last team from get_last_entity: {last_team}")
            except Exception as e:
                print(f"Error extracting last team from memory: {str(e)}")

        # If memory has context from previous queries, extract from there
        if hasattr(memory, 'get_last_context') and callable(memory.get_last_context):
            try:
                last_context = memory.get_last_context()
                if last_context and isinstance(last_context, dict):
                    if 'team_context' in last_context:
                        entities['last_team'] = last_context['team_context']
                        print(f"📝 Found team_context in last_context: {last_context['team_context']}")
                    elif 'team' in last_context:
                        entities['last_team'] = last_context['team']
                        print(f"📝 Found team in last_context: {last_context['team']}")
                    elif 'last_team' in last_context:
                        entities['last_team'] = last_context['last_team']
                        print(f"📝 Found last_team in last_context: {last_context['last_team']}")
            except Exception as e:
                print(f"Error extracting context from memory: {str(e)}")

        return entities

    def _validate_query(self, query_str: str, query_context: dict = None) -> tuple[bool, str]:
        """
        Use the LLM to validate if a query meets the minimal standard for SQL generation.

        This method sends the query to the LLM along with examples of good and bad queries,
        asking it to determine if the query is specific enough to generate meaningful SQL.
        If query_context is provided, it will be used to help validate follow-up queries.

        Args:
            query_str: The user's query to validate
            query_context: Optional context from previous interactions, including team references

        Returns:
            Tuple of (is_valid, reason)
            - is_valid: Boolean indicating if the query is valid
            - reason: Explanation of why the query is or isn't valid
        """
        logger.info(f"Validating query specificity: {query_str}")

        # Add context information if available
        context_section = ""
        if query_context:
            context_section = "CONVERSATION CONTEXT:\n"
            if 'team_context' in query_context:
                context_section += f"- Previously mentioned team: {query_context['team_context']}\n"
            if 'entity_mappings_from_memory' in query_context and query_context['entity_mappings_from_memory'].get('last_team'):
                context_section += f"- Last discussed team: {query_context['entity_mappings_from_memory']['last_team']}\n"
            context_section += "\n"

        # Create a prompt with examples of good and bad queries
        prompt = f"""
        Your task is to determine if a user query about soccer data is specific enough to generate meaningful SQL.

        EXAMPLES OF GOOD QUERIES (specific enough):
        - "Show me all matches where Key West FC played this month"
        - "What is Train Track Man FC's win-loss record?"
        - "How many goals did Sleigh All Day score in away games in February 2025?"
        - "List the top 10 teams by total goals scored"
        - "What teams are in Division 3?"
        - "How is their win percentage this month?" (when 'their' clearly refers to a specific team from context)

        EXAMPLES OF BAD QUERIES (too vague):
        - "Tell me about soccer"
        - "Show me stats"
        - "Who is nc"
        - "What about teams"
        - "Tell me more"

        A good query should:
        1. Reference specific entities (teams, divisions, players) OR specific metrics (goals, wins, etc.)
        2. Have clear intent that could be translated to SQL (listing, counting, comparing, etc.)
        3. Not be so vague that it could be interpreted in many different ways
        4. If it uses pronouns like "they" or "their", the context should make it clear what entity is being referenced

        {context_section}
        User query: "{query_str}"

        First, determine if this query is specific enough to generate meaningful SQL (YES or NO).
        Then, provide a brief explanation of your reasoning.
        Format your answer as:
        VALID: YES/NO
        REASON: your explanation
        """

        # Get response from LLM
        response = self.llm.complete(prompt)
        response_text = response.text.strip() if response and hasattr(response, 'text') else ""

        # Parse the response
        is_valid = "VALID: YES" in response_text.upper()

        # Extract the reason from the response
        reason_match = re.search(r"REASON:(.*?)($|VALID:)", response_text, re.DOTALL | re.IGNORECASE)
        reason = reason_match.group(1).strip() if reason_match else "No explanation provided"

        logger.info(f"Query validation result: valid={is_valid}, reason={reason}")

        return is_valid, reason

    def _load_additional_context(self, memory):
        """
        Load additional context from memory if available.

        This method consolidates all the context loading operations that were previously
        scattered throughout the query method.

        Args:
            memory: The memory object for conversation context

        Returns:
            Dictionary of context information or None if no context available
        """
        if not memory:
            return None

        context = {}

        # Get session ID
        session_id = self._get_or_create_session_id(memory)

        # Load the most recent query context from memory
        if hasattr(memory, 'get_last_query_context') and callable(memory.get_last_query_context):
            memory_context = memory.get_last_query_context(session_id)
            if memory_context:
                context.update(memory_context)

        # First try to find if there's an explicit team being discussed
        team_context = self._find_team_in_conversation_history(memory, session_id)
        if team_context:
            logger.info(f"📝 Found team context from conversation history: {team_context}")
            context["team_context"] = team_context

        # Then extract all entity mappings from memory
        entity_mappings = self._extract_entities_from_memory(memory)
        if entity_mappings:
            logger.info(f"📝 Extracted entity mappings from memory: {entity_mappings}")
            context["entity_mappings_from_memory"] = entity_mappings

        return context if context else None