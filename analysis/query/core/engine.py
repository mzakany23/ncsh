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

        # First, see if the query mentions a division
        division_pattern = r'\b(?:division|league|div|d)[\s.]*([\dIVXC]+|one|two|three|four|five|1st|2nd|3rd|4th|5th|c)\b'
        division_match = re.search(division_pattern, query, re.IGNORECASE)

        division_context = ""
        if division_match:
            division_text = division_match.group(1).lower()
            print(f"📝 Detected division reference: {division_match.group(0)}")

            # Map text divisions to numbers
            division_mapping = {
                'one': '1', '1st': '1', 'c': '1',  # Map 'c' to division 1 based on your conversation example
                'two': '2', '2nd': '2',
                'three': '3', '3rd': '3',
                'four': '4', '4th': '4',
                'five': '5', '5th': '5',
            }

            # Try to map the detected division to a number
            division_number = division_mapping.get(division_text, division_text)

            # Handle roman numerals if needed
            if division_text.upper() in ['I', 'II', 'III', 'IV', 'V']:
                roman_mapping = {'I': '1', 'II': '2', 'III': '3', 'IV': '4', 'V': '5'}
                division_number = roman_mapping.get(division_text.upper(), division_text)

            # Get available divisions for context
            available_divisions = get_available_divisions(self.sql_database)
            if available_divisions:
                division_context = f"Available divisions in the database are: {', '.join(available_divisions)}.\n"
                division_context += f"The query mentions division {division_number}.\n"
                # Add division to context
                query_context["division"] = division_number

        # Get team/division information for context
        team_info = get_teams_by_division(self.sql_database)
        team_division_context = team_info.get("division_context", "")

        # Check if the query mentions a specific team
        team_context = ""
        team_match = find_best_matching_team(query, self.teams)
        if team_match:
            matched_phrase, team_name = team_match
            print(f"📝 Detected team reference: '{matched_phrase}' matched to '{team_name}'")
            team_context = f"The query mentions the team '{team_name}'.\n"
            query_context["team"] = team_name

        # Check if the query mentions time periods
        time_context = ""
        if re.search(r'\b(this|current|present)\s+(month|year|week)\b', query, re.IGNORECASE):
            time_period = re.search(r'\b(month|year|week)\b', query, re.IGNORECASE).group(1).lower()
            time_context = f"The query asks about the current {time_period}.\n"
            time_context += f"Current date function to use: CURRENT_DATE\n"
            time_context += f"For current {time_period}, use: date >= DATE_TRUNC('{time_period}', CURRENT_DATE) AND date < DATE_TRUNC('{time_period}', CURRENT_DATE) + INTERVAL '1 {time_period}'\n"
            query_context["time_period"] = time_period

        # Add table schema context
        table_context = self._get_table_context()

        # Instructions for LLM
        sql_generation_prompt = f"""You are a helpful database assistant with expertise in soccer matches that translates questions into SQL queries for a DuckDB database.

DATABASE SCHEMA:
{table_context}

IMPORTANT CONTEXT AND INSTRUCTIONS:
{division_context}
{team_division_context}
{team_context}
{time_context}

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
- When a query mentions a specific team like "Key West", use LIKE pattern matching for both home_team and away_team:
  WHERE (home_team LIKE '%Key West%' OR away_team LIKE '%Key West%')

4. NULL SCORE HANDLING:
- Some matches have NULL scores because they haven't been played yet.
- When calculating win/loss/draw records, treat NULL scores as "upcoming" or "not played" matches.
- Use IS NULL checks to identify upcoming matches: WHERE home_score IS NULL OR away_score IS NULL
- For match result classifications, include a case for upcoming matches:
  CASE
    WHEN home_score IS NULL OR away_score IS NULL THEN 'Upcoming'
    WHEN home_team LIKE '%Team%' AND home_score > away_score THEN 'Win'
    ... (other cases)
  END AS result

5. SQL FORMATTING:
- Ensure the SQL is valid DuckDB syntax
- Always use Common Table Expressions (CTEs) with WITH statements to make the query readable
- Always end with a semicolon
- Order results appropriately based on the query (e.g., by points, goals, dates)
- Limit results to a reasonable number (e.g., 10-20) unless specifically asked for more
- Include a LIMIT clause at the end of any query that may return many rows

6. CALCULATIONS:
- For standings or league tables:
  * Calculate matches_played as count of matches with non-NULL scores
  * Calculate wins, draws, and losses based on results
  * Calculate points as 3*wins + 1*draws
  * Calculate goal difference (GD) as (goals_for - goals_against)

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
        Process a user query and return a natural language response.

        Args:
            query_str: The user query
            memory: Optional memory object for conversation context

        Returns:
            Natural language response as a string
        """
        print(f"📥 Processing query: '{query_str}'")

        # Store memory for later use
        self.memory = memory

        # Use clean_query to remove formatting requests for LLM processing
        clean_query = self._clean_query(query_str)
        if clean_query != query_str:
            print(f"Cleaned query (formatting requests removed): '{clean_query}'")

        # Initialize query context
        query_context = {}

        try:
            # Generate SQL from the query
            inferred_sql, updated_context = self._infer_query_and_generate_sql(clean_query, query_context)

            # Execute the SQL
            print(f"🔍 Executing SQL: {inferred_sql}")

            # Check if SQL contains error message
            if inferred_sql and ("SELECT 'Failed to generate" in inferred_sql or "SELECT 'Error" in inferred_sql):
                error_msg = inferred_sql.replace("SELECT '", "").replace("' AS error;", "")
                return f"I'm sorry, I couldn't generate a valid SQL query for that question. {error_msg}"

            # Execute the SQL query
            results = self.sql_database.run_sql(inferred_sql)

            # Check for empty results
            if is_empty_result(results):
                print("⚠️ Warning: Query returned empty results")

            # Check for unrealistic values
            if has_unrealistic_values(results):
                print("⚠️ Warning: Query returned potentially unrealistic values for soccer statistics")
                # Add a warning flag to the context for the response generation
                updated_context['unrealistic_values'] = True

            # Add debugging for the results
            print(f"\n--- SQL Results Debug ---")
            print(f"Results type: {type(results)}")
            print(f"Results content (sample): {str(results)[:500] if results else 'No results'}")
            print(f"--- End SQL Results Debug ---\n")

            # Store memory context
            self.memory_context = updated_context

            # Use LLM to infer and format the response
            response = self._infer_response(query_str, results, updated_context)
            return response

        except Exception as e:
            error_message = f"An error occurred while processing your query: {str(e)}"
            print(f"❌ {error_message}")
            return error_message

    def reset_memory(self):
        """Reset the conversation memory."""
        self.memory_context = None