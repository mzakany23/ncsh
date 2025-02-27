"""Core Query Engine functionality.

This module contains the QueryEngine class and related functions
that implement the inference-based approach to SQL generation and response formatting.
"""

import os
import re
import json
from typing import Dict, List, Any, Optional, Tuple, Union

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

        # Add format instructions based on query context
        format_instructions = """
        Format your response in a conversational and helpful manner. If the results include:
        - Rankings: Present as a numbered list
        - Statistics: Include the most important metrics
        - Match results: Organize them clearly with proper formatting
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

        Your response should read as if you're directly answering the user's original question.
        """

        # Get the response from Claude
        response = self.llm.complete(prompt)

        # Return the formatted response
        return response.text.strip()

    def _infer_query_and_generate_sql(self, query: str, query_context: dict) -> Tuple[str, dict]:
        """
        Use the LLM to infer the intention and generate a SQL query.

        Args:
            query: The user query
            query_context: Context from memory and query processing

        Returns:
            Tuple of (SQL query, updated context)
        """
        print("🧠 Generating SQL with LLM")

        # Add table schema context
        table_context = self._get_table_context()

        # Get team/division information for context
        team_info = get_teams_by_division(self.sql_database)
        team_division_context = team_info.get("division_context", "")

        # Retrieve conversation history if memory is available
        conversation_history = ""
        if hasattr(self, 'memory') and self.memory:
            if hasattr(self.memory, 'format_context'):
                # Get formatted conversation history
                formatted_history = self.memory.format_context(getattr(self.memory, 'session_id', None))
                if formatted_history:
                    # Limit history size if needed
                    if len(formatted_history) > 4000:
                        print("📝 Conversation history is large, truncating to recent exchanges")
                        # Get individual exchanges
                        exchanges = formatted_history.split("\n")
                        # Keep only the most recent exchanges (last 3-5 turns)
                        recent_exchanges = exchanges[-10:]  # Adjust number as needed
                        formatted_history = "\n".join(recent_exchanges)

                    conversation_history = f"""
                    CONVERSATION HISTORY (Most recent exchanges):
                    {formatted_history}

                    Based on this conversation history, the user is continuing the conversation.
                    If there are references to teams, divisions, or time periods in the history,
                    make sure to maintain that context in the current query.
                    """
                    print("📝 Including conversation history for context")

        # Detect if current query explicitly mentions divisions or teams
        # This helps prioritize explicit mentions over implied context
        explicit_division = re.search(r'\b(?:division|league|div|d)[\s.]*([\dIVXC]+|one|two|three|four|five|1st|2nd|3rd|4th|5th|c)\b', query, re.IGNORECASE)
        explicit_team = find_best_matching_team(query, self.teams)

        # Create context summary string based on the current query and conversation history
        context_summary = "QUERY CONTEXT SUMMARY:\n"

        # Add explicit mentions from current query first (highest priority)
        if explicit_division:
            division_text = explicit_division.group(1).lower()
            # Map text divisions to numbers (simplified mapping logic here)
            division_mapping = {
                'one': '1', '1st': '1', 'c': '1',
                'two': '2', '2nd': '2',
                'three': '3', '3rd': '3',
                'four': '4', '4th': '4',
                'five': '5', '5th': '5',
            }
            division_number = division_mapping.get(division_text, division_text)
            context_summary += f"- The current query explicitly mentions division {division_number}\n"
            query_context["division"] = division_number

        if explicit_team:
            matched_phrase, team_name = explicit_team
            context_summary += f"- The current query explicitly mentions team '{team_name}'\n"
            query_context["team"] = team_name

        # Add previous context if not overridden by explicit mentions
        if not explicit_team and "team" in query_context:
            team_name = query_context["team"]
            context_summary += f"- Previous context indicates this is about team '{team_name}'\n"
            # Check if we have follow-up indicators
            follow_up_pronouns = ['they', 'their', 'them', 'these', 'those', 'it', 'its', 'who']
            if any(pronoun in query.lower().split() for pronoun in follow_up_pronouns):
                context_summary += f"- The query uses pronouns that likely refer to '{team_name}'\n"

        if not explicit_division and "division" in query_context:
            division_number = query_context["division"]
            context_summary += f"- Previous context indicates this is about division {division_number}\n"

        # Check if query mentions time periods
        time_period_match = re.search(r'\b(this|current|present)\s+(month|year|week)\b', query, re.IGNORECASE)
        if time_period_match:
            time_period = re.search(r'\b(month|year|week)\b', query, re.IGNORECASE).group(1).lower()
            context_summary += f"- The query mentions the current {time_period}\n"
            query_context["time_period"] = time_period
        elif "time_period" in query_context:
            time_period = query_context["time_period"]
            context_summary += f"- Previous context indicates this is about the current {time_period}\n"

        # Check for upcoming/not played matches
        not_played_patterns = [
            r'\bnot\s+played\b', r'\bhaven\'t\s+played\b', r'\bstill\s+have\s+to\s+play\b',
            r'\bupcoming\b', r'\bscheduled\b', r'\bfuture\b', r'\bhave\s+left\b', r'\bnext\s+(game|match)\b',
            r'\bplay\s+against\s+next\b', r'\bwho\s+do\s+they\s+play\b'
        ]

        is_upcoming_match_query = any(re.search(pattern, query, re.IGNORECASE) for pattern in not_played_patterns)
        if is_upcoming_match_query:
            context_summary += "- The query is about upcoming matches that haven't been played yet\n"

        # Instructions for LLM
        sql_generation_prompt = f"""You are a helpful database assistant with expertise in soccer matches that translates questions into SQL queries for a DuckDB database.

DATABASE SCHEMA:
{table_context}

{conversation_history}

{context_summary}

TEAM DIVISION INFO:
{team_division_context}

IMPORTANT INSTRUCTIONS:

1. DIVISION HANDLING:
- When a query references a division (like "division 3", "league 2", or "C league"), you MUST include filters for both home_team and away_team to include teams from that division.
- Divisions are typically denoted in parentheses at the end of team names like "Team Name (3)" where "3" is the division.
- Use REGEXP_EXTRACT and filtering patterns like this:
  WHERE (REGEXP_EXTRACT(home_team, '\\\\(([A-Za-z0-9])\\\\)', 1) = '[division]' OR REGEXP_EXTRACT(away_team, '\\\\(([A-Za-z0-9])\\\\)', 1) = '[division]')
- Note: "C league" refers to division 1.

2. DATE HANDLING:
- For "current month" or "this month", use a filter like:
  WHERE date >= DATE_TRUNC('month', CURRENT_DATE) AND date < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'
- Similarly for "current year", "this week", etc.
- ALWAYS include the month filter when a query mentions "this month".

3. TEAM HANDLING:
- When a query is about a specific team (whether mentioned explicitly or implied through conversation history):
  * ALWAYS use LIKE pattern matching for both home_team and away_team:
    WHERE (home_team LIKE '%Team Name%' OR away_team LIKE '%Team Name%')
  * For follow-up questions using pronouns like "they" or "their", apply appropriate team filters based on context
  * If the conversation context mentions a specific team, make sure to filter for that team

4. NULL SCORE HANDLING:
- Some matches have NULL scores because they haven't been played yet.
- When calculating win/loss/draw records, treat NULL scores as "upcoming" or "not played" matches.
- Use IS NULL checks to identify upcoming matches: WHERE home_score IS NULL OR away_score IS NULL
- For match result classifications, include a case for upcoming matches

5. SQL FORMATTING:
- Ensure the SQL is valid DuckDB syntax
- Always use Common Table Expressions (CTEs) with WITH statements to make the query readable
- Always end with a semicolon
- Order results appropriately based on the query (e.g., by points, goals, dates)
- Limit results to a reasonable number (e.g., 10-20) unless specifically asked for more
- Include a LIMIT clause at the end of any query that may return many rows

YOUR TASK:
1. Translate the following query into a SQL query for DuckDB
2. Focus ONLY on generating the SQL query; do not provide explanations
3. Use clear SQL formatting with line breaks and proper indentation
4. Your response should ONLY contain the SQL query, nothing else

USER QUERY: {query}

SQL QUERY:"""

        # Generate the SQL query
        response = self.llm.complete(sql_generation_prompt)
        sql_result = response.text.strip()
        print(f"🔍 Generated SQL: {sql_result}")

        # Clean up the SQL result
        sql_query = fix_duckdb_sql(sql_result)

        # Update context with query type if we can determine it
        if "standings" in query.lower() or "table" in query.lower() or "best" in query.lower():
            query_context["query_type"] = "standings"
        elif "score" in query.lower() or "goals" in query.lower():
            query_context["query_type"] = "goals"
        elif "match" in query.lower() or "played" in query.lower():
            query_context["query_type"] = "matches"
        else:
            query_context["query_type"] = "general"

        return sql_query, query_context

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
        self.memory_context = {}

        # Initialize query context
        query_context = {}

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

        # Store context for potential follow-up queries
        self.memory_context = query_context

        # Run the query
        try:
            results = self.sql_database.run_sql(sql_query)

            # Check for empty/unrealistic results
            if is_empty_result(results):
                print("⚠️ Query returned empty results")
                # If the query mentions "not played" but doesn't correctly filter for NULL scores,
                # try modifying the query to handle this case
                if (
                    "not played" in clean_query.lower()
                    or "upcoming" in clean_query.lower()
                    or "next" in clean_query.lower()
                ) and "IS NULL" not in sql_query:
                    print("🔄 Attempting to modify query to handle unplayed matches...")
                    modified_sql = sql_query.replace(";", " AND (home_score IS NULL OR away_score IS NULL);")
                    query_context["sql"] = modified_sql
                    results = self.sql_database.run_sql(modified_sql)

            if has_unrealistic_values(results):
                print("⚠️ Query returned potentially unrealistic values (like too many goals)")

        except Exception as e:
            print(f"❌ Error executing SQL query: {str(e)}")
            return f"I encountered an error executing the SQL query: {str(e)}"

        # Infer a natural language response
        try:
            inferred_response = self._infer_response(clean_query, results, query_context)
            return inferred_response
        except Exception as e:
            print(f"❌ Error inferring response: {str(e)}")
            # Fallback response with the raw results
            return f"I found some results but couldn't generate a proper explanation. Here are the raw results:\n\n{results}"

    def reset_memory(self):
        """Reset the conversation memory."""
        self.memory_context = None