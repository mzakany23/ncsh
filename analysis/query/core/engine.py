"""Core Query Engine functionality.

This module contains the QueryEngine class and related functions
that implement the inference-based approach to SQL generation and response formatting.
"""

import os
import re
import json
from typing import Dict, List, Any, Optional, Tuple, Union
from datetime import datetime, date  # Add date import

from llama_index.core.indices.struct_store import NLSQLTableQueryEngine
from llama_index.core.response import Response

from ..sql.database import DuckDBSQLDatabase
from ..utils.sql_helpers import fix_duckdb_sql
from ..utils.validation import is_empty_result, has_unrealistic_values
from ..utils.team_info import (
    get_all_teams,
    find_best_matching_team,
    get_teams_by_division,
    get_available_divisions
)


class QueryEngine(NLSQLTableQueryEngine):
    """
    Custom query engine that extends LlamaIndex's NLSQLTableQueryEngine for soccer match data.
    Handles SQL generation, execution, and response formatting with specialized features
    for soccer data analysis.
    """

    def __init__(self, sql_database, llm, always_infer=True, **kwargs):
        """
        Initialize with SQL database and LLM.

        Args:
            sql_database: SQL database connection
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

        return query_str.strip()

    def _infer_response(self, query_str: str, results, query_context: dict) -> str:
        """
        Generate a natural language response based on the query and results.

        Args:
            query_str: The original user query
            results: The query results
            query_context: Additional context including metadata about the query

        Returns:
            Natural language response as a string
        """
        print("🧠 Generating response with LLM")

        # Determine the query type for better context
        query_type = query_context.get('query_type', 'general')

        # Extract date information from context
        current_date = query_context.get('current_date', 'Unknown')
        current_year = query_context.get('current_year', 'Unknown')
        current_month = query_context.get('current_month', 'Unknown')

        # Format the results for the prompt
        # Truncate if too long to avoid prompt size issues
        result_str = str(results)
        if len(result_str) > 3000:
            result_str = result_str[:3000] + "... [truncated due to length]"

        # Add specialized instructions based on query context
        specialized_instructions = ""

        if is_empty_result(results):
            specialized_instructions += """
            IMPORTANT: The query returned no results. You should:
            1. Clearly state that no data was found
            2. Suggest possible reasons for the lack of results
            3. Recommend alternative queries that might yield results
            4. DO NOT make up data that doesn't exist in the results
            """

        # Add time context instructions
        time_context_instructions = f"""
        IMPORTANT DATE CONTEXT:
        - Today's date is {current_date}
        - Current year is {current_year}
        - Current month is {current_month}

        When interpreting results:
        - Matches with dates in the future (after {current_date}) and NULL scores are upcoming matches
        - Matches with dates in the past (before {current_date}) but NULL scores may be postponed or canceled
        - Matches with dates in the past (before {current_date}) with recorded scores are completed matches

        Make sure to clearly indicate which matches are upcoming vs. completed.
        """

        # Add format instructions based on query context
        format_instructions = """
        Format your response in a conversational and helpful manner. If the results include:
        - Rankings: Present as a numbered list
        - Statistics: Include the most important metrics
        - Match results: Organize them clearly with proper formatting
        - Upcoming matches: Group them by date and clearly indicate they haven't been played yet
        """

        # Add data validity instructions
        data_validity_instructions = ""
        if query_context.get("unrealistic_values", False):
            data_validity_instructions = """
            WARNING: The query results contain unrealistically high values for soccer statistics.
            In real soccer matches, a typical team scores between 0-7 goals per game, and total goals
            for a team over a month might range from 0-30. Please mention this anomaly in your response
            and suggest that the user verify the data or refine their query.
            """

        # Build the prompt for the LLM
        prompt = f"""
        I need you to create a natural language response to summarize the results of a database query.

        USER QUERY: "{query_str}"

        QUERY TYPE: {query_type}

        QUERY RESULTS:
        {result_str}

        {specialized_instructions}

        {time_context_instructions}

        {format_instructions}

        {data_validity_instructions}

        INSTRUCTIONS:
        1. Analyze the query results and create a natural, informative response that directly answers the user's question
        2. Format your response according to any format preferences detected in the query
        3. Make sure your response is accurate based on the data provided
        4. If the results are empty or contain an error message, acknowledge this and explain what might be missing
        5. Keep your response concise but complete
        6. Include key statistics and insights that would be most relevant to the user's question
        7. Do not include technical details about SQL or internal processing
        8. When discussing upcoming matches, always mention the current date for reference

        Your response should read as if you're directly answering the user's original question.
        """

        # Get the response from Claude
        response = self.llm.complete(prompt)

        # Return the formatted response
        return response.text.strip()

    def _infer_query_and_generate_sql(self, query: str, query_context: dict) -> Tuple[str, dict]:
        """
        Infer SQL query from user query using LLM.
        Returns the SQL query and additional context information.
        """
        # Add current date to context
        today = date.today()
        current_date_str = today.strftime("%Y-%m-%d")
        query_context["current_date"] = current_date_str
        query_context["current_year"] = today.year
        query_context["current_month"] = today.month
        query_context["current_day"] = today.day

        # Get prompt components
        prompt_context = self._get_table_context()
        cleaned_query = self._clean_query(query)

        # Get conversation history if available
        conversation_history = ""
        conversation_summary = ""
        if "memory" in query_context and query_context["memory"]:
            memory = query_context["memory"]
            session_id = query_context.get("session_id")
            if session_id and hasattr(memory, "format_context"):
                try:
                    conversation_history = memory.format_context(session_id)
                    if hasattr(memory, "summarize_context"):
                        conversation_summary = memory.summarize_context(session_id)
                except Exception as e:
                    print(f"⚠️ Error getting conversation history: {str(e)}")

        # Identify team context
        team_context = None
        if "team" in query_context:
            team_context = query_context["team"]
        elif "memory" in query_context and query_context["memory"]:
            memory = query_context["memory"]
            session_id = query_context.get("session_id")
            if session_id and hasattr(memory, "get_last_team"):
                try:
                    team_context = memory.get_last_team(session_id)
                except Exception as e:
                    print(f"⚠️ Error getting team context: {str(e)}")

        # Check for follow-up questions about team
        follow_up_pronouns = ["they", "them", "their", "it", "its"]
        follow_up_keywords = ["recent", "last", "next", "upcoming", "schedule",
                             "against", "versus", "vs", "play", "match", "score",
                             "performance", "record", "standing"]

        # Check if this is likely a follow-up question
        likely_follow_up = False
        if team_context:
            # Check if query contains follow-up pronouns
            if any(pronoun in query.lower().split() for pronoun in follow_up_pronouns):
                likely_follow_up = True

            # Check if query contains follow-up keywords without mentioning "team"
            if any(keyword in query.lower() for keyword in follow_up_keywords) and "team" not in query.lower():
                likely_follow_up = True

        # Set follow-up context if this seems like a follow-up question about the team
        follow_up_context = ""
        if likely_follow_up and team_context:
            follow_up_context = f"""IMPORTANT: This appears to be a follow-up question about {team_context}.
Even though the team name is not explicitly mentioned in this query,
you MUST include filters for {team_context} in your SQL query.
If the query uses pronouns like 'they', 'them', or 'their', these refer to {team_context}."""

        # Check for questions about matches that haven't been played yet
        not_played_pattern = r'\b(?:not played|unplayed|upcoming|scheduled|future|next|coming|soon)\b'
        not_played_match = re.search(not_played_pattern, query.lower())

        not_played_context = ""
        if not_played_match or "upcoming" in query.lower():
            not_played_context = f"""IMPORTANT: This query is asking about matches that haven't been played yet.
Make sure to filter for matches where scores are NULL and the match date is greater than or equal to {current_date_str}.
Use conditions like: WHERE (home_score IS NULL OR away_score IS NULL) AND match_date >= '{current_date_str}'"""

        # Format the time context for the LLM
        time_context = f"""
CURRENT DATE CONTEXT:
- Today's date: {current_date_str}
- Current year: {today.year}
- Current month: {today.month}
- Current day: {today.day}

When determining "upcoming" or "scheduled" matches:
- Upcoming matches should have: match_date >= '{current_date_str}' AND (home_score IS NULL OR away_score IS NULL)
- Past matches should have: match_date < '{current_date_str}' OR (home_score IS NOT NULL AND away_score IS NOT NULL)
"""

        # Build the final prompt
        prompt = f"""You are a SQL query generator for a soccer match database. Given the user's question, generate a SQL query that will answer it.

{prompt_context}

{time_context}

{conversation_summary if conversation_summary else ""}

{conversation_history if conversation_history else ""}

{follow_up_context if follow_up_context else ""}

{not_played_context if not_played_context else ""}

USER QUERY: {cleaned_query}

Based on the user query and all available context, generate a SQL query that will correctly answer the question.
If the query is about a specific team, make sure to filter for that team.
If it's a follow-up question, be sure to maintain context from previous interactions.
Remember to check whether matches should be filtered by date, division, or scores.

The SQL query should:
1. Be valid DuckDB SQL syntax
2. Only include tables and columns that exist in the schema
3. Filter appropriately based on all context
4. Aliased columns when appropriate
5. Be formatted for readability

SQL QUERY:"""

        # Call the LLM to generate the SQL
        print(f"🔍 Generating SQL for query: {cleaned_query}")

        # Store context for future reference
        result_context = {
            "team": team_context,
            "is_follow_up": likely_follow_up,
            "not_played_context": bool(not_played_context),
            "current_date": current_date_str,
        }

        # Call LLM to generate SQL
        response = None
        try:
            response = self.llm.complete(prompt)
            if response and hasattr(response, "text"):
                inferred_sql = response.text
            else:
                inferred_sql = str(response)

            # Clean up SQL - remove markdown syntax
            inferred_sql = re.sub(r'^```sql\n', '', inferred_sql)
            inferred_sql = re.sub(r'^```\n', '', inferred_sql)
            inferred_sql = re.sub(r'\n```$', '', inferred_sql)

            # Store the generated SQL in the context
            result_context["generated_sql"] = inferred_sql

            return inferred_sql, result_context
        except Exception as e:
            print(f"⚠️ Error generating SQL: {str(e)}")
            return f"SELECT 'Error generating SQL: {str(e)}' as error_message;", result_context

    def query(self, query_str: str, memory=None) -> str:
        """
        Process a natural language query and return a response.

        Args:
            query_str: The natural language query string
            memory: Optional ConversationMemory instance for context

        Returns:
            Natural language response
        """
        print("\n🔍 Processing query:", query_str)

        # Clean query
        clean_query = self._clean_query(query_str)

        # Store memory for later use
        self.memory = memory

        # Initialize query context
        query_context = {"memory": memory}

        # If memory exists, try to get previous context (for follow-up queries)
        if memory:
            session_id = getattr(memory, 'session_id', None)
            if not session_id:
                # Get the session ID from memory if it's available
                if hasattr(memory, 'sessions') and memory.sessions:
                    # Take the most recent session if multiple exist
                    session_id = list(memory.sessions.keys())[0]

            # Store the session ID for later reference
            memory.session_id = session_id
            query_context["session_id"] = session_id

            print(f"📝 Using memory session: {session_id}")

            # Try to load last team and division from memory
            if hasattr(memory, 'get_last_team'):
                last_team = memory.get_last_team(session_id)
                if last_team:
                    query_context["team"] = last_team
                    print(f"📝 Retrieved last team from memory: {last_team}")

            if hasattr(memory, 'get_last_division'):
                last_division = memory.get_last_division(session_id)
                if last_division:
                    query_context["division"] = last_division
                    print(f"📝 Retrieved last division from memory: {last_division}")

            # Get previous query context if available (for multi-turn conversations)
            if hasattr(memory, 'get_last_query_context'):
                last_context = memory.get_last_query_context(session_id)
                if last_context:
                    # Only update specific fields, don't override the whole context
                    # This preserves the current query information
                    for key, value in last_context.items():
                        # Don't update these fields from previous context
                        if key not in ['sql', 'query']:
                            query_context[key] = value
                    print(f"📝 Retrieved additional context from memory: {last_context}")

        # Try to infer the SQL query
        try:
            sql_query, updated_context = self._infer_query_and_generate_sql(clean_query, query_context)
            query_context.update(updated_context)
            query_context["sql"] = sql_query
        except Exception as e:
            print(f"❌ Error generating SQL query: {str(e)}")
            return f"I encountered an error generating the SQL query: {str(e)}"

        # Store the original query in context
        query_context["query"] = clean_query

        # Run the query
        try:
            print(f"🔍 Executing SQL: {sql_query}")
            results = self.sql_database.run_sql(sql_query)

            # Check for empty/unrealistic results
            if is_empty_result(results):
                print("⚠️ Query returned empty results")
                # If we have date context and the query might be about upcoming matches,
                # try modifying the query to handle this case
                if (
                    "current_date" in query_context and
                    ("not played" in clean_query.lower()
                    or "upcoming" in clean_query.lower()
                    or "next" in clean_query.lower())
                ) and "IS NULL" not in sql_query:
                    print("🔄 Attempting to modify query for unplayed matches...")
                    current_date = query_context["current_date"]
                    modified_sql = sql_query.replace(";",
                        f" AND (home_score IS NULL OR away_score IS NULL) AND date >= '{current_date}';")
                    query_context["sql"] = modified_sql
                    print(f"🔍 Executing modified SQL: {modified_sql}")
                    results = self.sql_database.run_sql(modified_sql)

            if has_unrealistic_values(results):
                print("⚠️ Query returned potentially unrealistic values (like too many goals)")
                query_context["unrealistic_values"] = True

        except Exception as e:
            print(f"❌ Error executing SQL query: {str(e)}")
            return f"I encountered an error executing the SQL query: {str(e)}"

        # Infer a natural language response
        try:
            inferred_response = self._infer_response(clean_query, results, query_context)

            # Save interaction to memory if available
            if memory and hasattr(memory, 'add_interaction'):
                try:
                    memory.add_interaction(
                        session_id=getattr(memory, 'session_id', None),
                        query=clean_query,
                        response=inferred_response,
                        context=query_context
                    )
                except Exception as e:
                    print(f"⚠️ Error saving to memory: {str(e)}")

            return inferred_response
        except Exception as e:
            print(f"❌ Error inferring response: {str(e)}")
            # Fallback response with the raw results
            return f"I found some results but couldn't generate a proper explanation. Here are the raw results:\n\n{results}"

    def reset_memory(self):
        """Reset the conversation memory."""
        self.memory_context = None