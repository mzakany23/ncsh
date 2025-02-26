import os
from pathlib import Path
import boto3
import argparse
from sqlalchemy import create_engine, text
from llama_index.core import SQLDatabase
from llama_index.core.indices.struct_store import NLSQLTableQueryEngine
from llama_index.llms.openai import OpenAI
from llama_index.llms.anthropic import Anthropic
from thefuzz import fuzz
import re
from llama_index.core.response import Response
from memory import ConversationMemory
from datetime import datetime
import json
import logging
import traceback

# Initialize conversation memory
memory_manager = ConversationMemory()


def download_db_if_not_exists():
    """Download the DuckDB parquet file from S3 if it doesn't exist locally."""
    db_path = Path("matches.parquet")
    if not db_path.exists():
        print("Downloading database from S3...")
        session = boto3.Session(profile_name='mzakany')
        s3 = session.client('s3', region_name='us-east-2')
        try:
            s3.download_file(
                'ncsh-app-data',
                'data/parquet/data.parquet',
                str(db_path)
            )
            print("Download complete.")
        except Exception as e:
            print(f"Failed to download parquet file: {e}")
            raise
    else:
        print("Local parquet file exists. Using existing file.")
    return db_path


def setup_database():
    """Set up the DuckDB database connection and load parquet data into an in-memory table."""
    # Ensure we have the parquet file
    db_path = download_db_if_not_exists()

    # Create a DuckDB in-memory connection
    engine = create_engine("duckdb:///:memory:", future=True)

    # Create and register the table using the parquet file
    with engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS matches AS SELECT * FROM read_parquet('{db_path}')"))
        # Verify table exists and has data
        result = conn.execute(text("SELECT COUNT(*) FROM matches")).scalar()
        print(f"Loaded {result} matches into database")

    return engine


def get_all_teams(engine):
    """Get all unique team names from the database."""
    with engine.begin() as conn:
        home_teams = conn.execute(text("SELECT DISTINCT home_team FROM matches")).fetchall()
        away_teams = conn.execute(text("SELECT DISTINCT away_team FROM matches")).fetchall()

    # Combine and deduplicate team names
    teams = set(team[0] for team in home_teams + away_teams)
    # Clean up team names - remove (1), etc.
    teams = {re.sub(r'\s*\(\d+\)\s*$', '', team) for team in teams}
    return list(teams)


def find_best_matching_team(query, teams, threshold=80):
    """
    Find the best matching team name in the query using fuzzy matching.
    Returns (original_phrase, matched_team) if found, None otherwise.
    """
    # Skip common words and phrases that shouldn't be matched as team names
    skip_phrases = [
        'this year', 'last year', 'for this', 'their', 'they', 'the team',
        'of the', 'in the', 'by the', 'and the', 'with the', 'show me',
        'give me', 'tell me', 'list the', 'what are', 'who are',
        'which team', 'what team', 'top team', 'best team', 'worst team',
        'highest ranked', 'lowest ranked', 'team with', 'teams with',
        'team that', 'teams that', 'team who', 'teams who',
        'had the', 'most', 'least', 'highest', 'lowest',
        'team at', 'team in', 'teams at', 'teams in', 'team from', 'teams from'
    ]

    # Extract potential team name phrases (2-5 word combinations)
    words = query.lower().split()
    phrases = []
    for i in range(len(words)):
        for j in range(2, 6):  # Try phrases of length 2 to 5 words
            if i + j <= len(words):
                phrase = ' '.join(words[i:i+j])
                if not any(skip in phrase for skip in skip_phrases):
                    phrases.append(phrase)

    best_match = None
    best_score = 0
    best_phrase = None

    for phrase in phrases:
        # Skip if the phrase is too short or common
        if len(phrase) < 3 or phrase in skip_phrases:
            continue

        for team in teams:
            # Try both direct and token set ratio for better matching
            direct_score = fuzz.ratio(phrase, team.lower())
            token_score = fuzz.token_set_ratio(phrase, team.lower())
            score = max(direct_score, token_score)

            if score > best_score and score >= threshold:
                best_score = score
                best_match = team
                best_phrase = phrase

    return (best_phrase, best_match) if best_match else None


def preprocess_query(query, engine):
    """Preprocess the query to handle team name matching."""
    # First, check for division mentions before team matching
    # This helps prevent division identifiers from being matched as team names

    # Use more flexible patterns to match various division references
    division_patterns = [
        r'\b(?:division|div)[.\s]+([a-zA-Z0-9])\b',         # division c, div. c
        r'\b([a-zA-Z0-9])[.\s]+(?:division|div)\b',         # c division, c div.
        r'\b(?:league)[.\s]+([a-zA-Z0-9])\b',               # league c
        r'\b([a-zA-Z0-9])[.\s]+(?:league)\b',               # c league
        r'\b(?:the)\s+([a-zA-Z0-9])\s+(?:league|division|div)\b'  # the c league
    ]

    # Check if we have any division references
    modified_query = query
    print(f"🔍 Checking query for division references: '{query}'")
    division_placeholders = {}
    division_letter = None

    # Try each pattern
    for pattern in division_patterns:
        division_match = re.search(pattern, query, re.IGNORECASE)
        if division_match:
            # Extract the full match and the division letter
            full_match = division_match.group(0)
            # The division letter might be in group 1
            if len(division_match.groups()) > 0:
                division_letter = division_match.group(1).upper()

            print(f"✓ Found division reference: '{full_match}' -> Division '{division_letter}'")

            # Replace the division reference to prevent team matching
            placeholder = f"__DIVISION_{division_letter}__"
            division_placeholders[placeholder] = full_match
            modified_query = modified_query.replace(full_match, placeholder)
            break  # Found a match, no need to check other patterns

    # Now do team matching on the modified query
    teams = get_all_teams(engine)
    print(f"🔍 Checking for team matches in: '{modified_query}'")
    team_match = find_best_matching_team(modified_query, teams)

    if team_match:
        original_phrase, matched_team = team_match
        print(f"✓ Matched team name: '{original_phrase}' -> '{matched_team}'")
        # Replace the original phrase with the exact team name
        modified_query = re.sub(re.escape(original_phrase), matched_team, modified_query, flags=re.IGNORECASE)

    # Restore division references that we temporarily replaced
    for placeholder, original in division_placeholders.items():
        modified_query = modified_query.replace(placeholder, original)

    # If we identified a division but didn't make any changes via the placeholders,
    # add a hint to the query context
    if division_letter and query == modified_query:
        print(f"🔍 Division detected but no team replaced, adding division context: '{division_letter}'")

    print(f"🔍 Final preprocessed query: '{modified_query}'")
    return modified_query


def fix_duckdb_sql(sql_query):
    """Fix common SQL syntax issues for DuckDB compatibility."""
    if not sql_query:
        return sql_query

    # Fix DATEADD function
    dateadd_pattern = r"DATEADD\s*\(\s*'month'\s*,\s*-\d+\s*,\s*CURRENT_DATE\s*\)"
    sql_query = re.sub(
        dateadd_pattern,
        lambda m: "CURRENT_DATE - INTERVAL '1 month'",
        sql_query,
        flags=re.IGNORECASE
    )

    # Fix DATE_ADD function (another common variant)
    date_add_pattern = r"DATE_ADD\s*\(\s*CURRENT_DATE\s*,\s*INTERVAL\s*-\d+\s*MONTH\s*\)"
    sql_query = re.sub(
        date_add_pattern,
        lambda m: "CURRENT_DATE - INTERVAL '1 month'",
        sql_query,
        flags=re.IGNORECASE
    )

    return sql_query


class DuckDBSQLDatabase(SQLDatabase):
    """Custom SQLDatabase class that fixes DuckDB-specific SQL syntax."""

    def run_sql(self, sql_query: str, **kwargs):
        """Override run_sql to fix DuckDB syntax before execution."""
        fixed_query = fix_duckdb_sql(sql_query)
        if fixed_query != sql_query:
            print(f"\nFixed SQL query: {fixed_query}")
        return super().run_sql(fixed_query, **kwargs)


class CustomNLSQLTableQueryEngine(NLSQLTableQueryEngine):
    """Custom query engine that uses LLM for flexible query processing and formatting."""

    def __init__(self, sql_database, llm, always_infer=True, **kwargs):
        """Initialize with SQL database and LLM."""
        super().__init__(sql_database=sql_database, **kwargs)
        self.sql_database = sql_database
        self.llm = llm
        self.memory_context = None  # Initialize memory context attribute
        self.always_infer = always_infer  # Flag to force using dynamic inference for all queries
        print(f"Query engine initialized with always_infer={self.always_infer}")

    def _get_table_context(self) -> str:
        """Get the database schema context."""
        return """
        Table: matches
        Columns:
        - date (DATE): The date of the match
        - home_team (TEXT): Name of the home team
        - away_team (TEXT): Name of the away team
        - home_score (INTEGER): Goals scored by home team
        - away_score (INTEGER): Goals scored by away team

        Notes:
        - Team names may include division numbers in parentheses, e.g. "Team Name (2)"
        - Current date functions available: CURRENT_DATE
        - Date functions available:
          * EXTRACT(field FROM date)
          * date - INTERVAL
          * DATE_TRUNC('month', date)
          * DATE_TRUNC('year', date)

        Time Period Examples:
        - This month: date >= DATE_TRUNC('month', CURRENT_DATE)
        - This year: date >= DATE_TRUNC('year', CURRENT_DATE)
        - Last 30 days: date >= CURRENT_DATE - INTERVAL '30 days'
        - Specific year: EXTRACT(year FROM date) = <year>
        """

    def _clean_query(self, query_str):
        """Remove formatting requests and other instructions from the query."""
        # Define patterns for formatting requests
        format_patterns = {
            "table": ["as table", "in table", "show table", "table format"],
            "list": ["as list", "in list", "show list", "list format"],
            "json": ["as json", "in json", "show json", "json format"],
            "csv": ["as csv", "in csv", "show csv", "csv format"]
        }

        query_lower = query_str.lower()
        clean_query = query_str

        # Remove formatting instructions
        for format_type, patterns in format_patterns.items():
            for pattern in patterns:
                clean_query = re.sub(f"(?i){pattern}", "", clean_query)

        # Remove common connectors that might be left dangling
        clean_query = re.sub(r"(?i)(and|--|,|\?)\s*$", "", clean_query.strip())

        return clean_query.strip()

    def _infer_response(self, query_str: str, results, query_context: dict) -> str:
        """
        Use LLM to generate a natural language response summarizing the query results.
        This allows for more flexible and context-aware formatting of responses.
        """
        print("\n🔍 Using LLM to generate response...")

        # Determine if there's a specific format requested
        format_type = query_context.get('format', 'default')

        # Prepare result data for inclusion in the prompt
        result_str = ""
        if isinstance(results, tuple) and len(results) > 0 and isinstance(results[0], str):
            # Handle tuple results (common from DuckDB)
            try:
                # Try to parse the data more cleanly
                import ast
                data_rows = ast.literal_eval(results[0])
                if data_rows and isinstance(data_rows, list):
                    result_str = str(data_rows)
                else:
                    result_str = str(results)
            except:
                result_str = str(results)
        elif isinstance(results, list):
            # For regular list results
            result_str = str(results)
        else:
            # For other result types
            result_str = str(results)

        # Extract the query type and other context
        query_type = query_context.get('query_type', 'unknown')
        team_name = query_context.get('team', None)

        # Build specialized prompt based on the query type
        specialized_instructions = ""
        if 'best defense' in query_str.lower() or 'fewest goals' in query_str.lower():
            specialized_instructions = """
            For defensive statistics queries:
            - Highlight the team with the best defense (fewest goals conceded)
            - Include their average goals conceded per match
            - Mention how many matches they've played
            - List a few of the next best teams if relevant
            """
        elif 'team rankings' in query_type or 'standings' in query_str.lower():
            specialized_instructions = """
            For team ranking queries:
            - List the top teams in order
            - Include their records (wins-losses-draws)
            - Mention win percentages if available
            """
        elif team_name and ('stats' in query_type or 'record' in query_str.lower()):
            specialized_instructions = f"""
            For team statistics queries:
            - Provide {team_name}'s overall record
            - Include total goals scored and conceded
            - Calculate their goal difference
            - Mention their win percentage
            """

        # Format instructions based on requested format
        format_instructions = ""
        if format_type == 'table' or 'table format' in query_str.lower():
            format_instructions = """
            Present your response as a formatted markdown table with:
            - Clear column headers
            - Aligned data in columns
            - A title row above the table

            Example format:
            ## Teams with Best Defense

            Team | Goals Conceded | Matches | Avg Goals Conceded
            -----|----------------|---------|------------------
            Team A | 5 | 10 | 0.5
            Team B | 8 | 12 | 0.67
            """
        elif format_type == 'summary' or 'summary' in query_str.lower():
            format_instructions = """
            Present your response as a concise paragraph summary that highlights the key findings,
            without listing all the individual data points.
            """
        elif format_type == 'detailed' or 'detailed' in query_str.lower():
            format_instructions = """
            Present your response as a detailed analysis with sections:
            - A summary of the key findings
            - Detailed breakdown of the results
            - Any notable trends or outliers
            - Context or comparisons if relevant
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

    def query(self, query_str: str, memory=None):
        """Query the table with a natural language query."""
        try:
            print(f"📝 Processing query: '{query_str}'")
            # Store memory for later use
            self.memory = memory

            # Check if this is a clarification response to a previous question
            is_clarification = False
            if memory and memory.get_last_query_context():
                last_context = memory.get_last_query_context()
                if last_context.get('awaiting_clarification'):
                    is_clarification = True
                    print("📝 This appears to be a clarification response")
                    # Merge the clarification with the original query context
                    if 'clarification_type' in last_context:
                        if last_context['clarification_type'] == 'division':
                            # User is clarifying division
                            division_match = re.search(r'(?:division|div|league)\s*([a-zA-Z])\b', query_str.lower())
                            if division_match:
                                division_letter = division_match.group(1).upper()
                                print(f"🔍 User clarified the division as: Division {division_letter}")
                                last_context['division'] = division_letter
                                # Remove the awaiting_clarification flag
                                last_context.pop('awaiting_clarification', None)
                                # Use the stored original query with our new context
                                query_str = last_context.get('original_query', query_str)
                                query_context = last_context
                            else:
                                # Try to identify division letter from short response like "c" or "league c"
                                single_letter = re.search(r'\b([a-zA-Z])\b', query_str.lower())
                                if single_letter:
                                    division_letter = single_letter.group(1).upper()
                                    print(f"🔍 User clarified the division as: Division {division_letter}")
                                    last_context['division'] = division_letter
                                    # Remove the awaiting_clarification flag
                                    last_context.pop('awaiting_clarification', None)
                                    # Use the stored original query with our new context
                                    query_str = last_context.get('original_query', query_str)
                                    query_context = last_context

            # Use clean_query to remove formatting requests for LLM processing
            clean_query = self._clean_query(query_str)
            if clean_query != query_str:
                print(f"Cleaned query (formatting requests removed): '{clean_query}'")

            # Process the query with our new preprocessing method
            if not is_clarification or not 'query_context' in locals():
                clean_query, query_context = self._preprocess_query(clean_query)

            # Check for ambiguity in the query that needs clarification
            ambiguity_check = self._check_for_ambiguities(clean_query, query_context)
            if ambiguity_check:
                clarification_type, clarification_message = ambiguity_check

                # Store the context so we can use it when the user responds
                query_context['awaiting_clarification'] = True
                query_context['clarification_type'] = clarification_type
                query_context['original_query'] = query_str  # Store original for later use

                # Store memory context
                self.memory_context = query_context

                # Return the clarification request
                return clarification_message

            # Check if we should bypass predefined templates and always use dynamic inference
            if self.always_infer:
                print(f"🔄 Bypassing predefined templates and using dynamic inference (always_infer=True)")
                inferred_sql, updated_context = self._infer_query_and_generate_sql(
                    clean_query, query_context
                )

                print(f"\n\n📊 Using inferred SQL: {inferred_sql}")

                # Execute the dynamically generated SQL
                print(f"🔍 Executing SQL: {inferred_sql}")
                try:
                    # Check if SQL contains error message
                    if inferred_sql and ("SELECT 'Failed to generate" in inferred_sql or
                                        "SELECT 'Error" in inferred_sql):
                        error_msg = inferred_sql.replace("SELECT '", "").replace("' AS error;", "")
                        return f"I'm sorry, I couldn't generate a valid SQL query for that question. {error_msg}"

                    # Execute the SQL query
                    results = self.sql_database.run_sql(inferred_sql)

                    # Add debugging for the results
                    print(f"\n--- SQL Results Debug ---")
                    print(f"Results type: {type(results)}")
                    print(f"Results content (sample): {str(results)[:500] if results else 'No results'}")
                    if isinstance(results, list) and results:
                        print(f"First result type: {type(results[0])}")
                        print(f"First result keys: {results[0].keys() if hasattr(results[0], 'keys') else 'No keys'}")
                    print(f"--- End SQL Results Debug ---\n")

                    # Store memory context if needed
                    self.memory_context = updated_context

                    # Check if we got empty results and should suggest a clarification
                    if self._is_empty_result(results) and not is_clarification:
                        possible_clarification = self._suggest_clarification_for_empty_results(clean_query, query_context, inferred_sql)
                        if possible_clarification:
                            return possible_clarification

                    # Use LLM to infer and format the response
                    response = self._infer_response(query_str, results, updated_context)
                    return response
                except Exception as infer_error:
                    print(f"Error processing query: {str(infer_error)}")
                    traceback.print_exc()

                    # Check if this is a SQL syntax error
                    error_str = str(infer_error)
                    if "syntax error" in error_str.lower():
                        # Try to extract the SQL issue if possible
                        error_info = "The SQL query had syntax errors."

                        # Try a simplified query approach as fallback
                        print("🔄 Attempting to generate a simplified query...")
                        try:
                            # Create a simpler prompt for SQL generation
                            simplified_prompt = f"""
                            Generate a simple SQL query for this question: "{query_str}"

                            DATABASE SCHEMA:
                            Table: matches
                            Columns: date, home_team, away_team, home_score, away_score

                            Keep the query as simple as possible. Focus only on the core question.
                            Ensure the query is complete, with all closing parentheses and semicolon.
                            DO NOT include any code block markers like ```sql or ```.
                            ONLY return the raw SQL with no formatting or explanation.
                            """

                            # Get the simplified SQL and clean it
                            simplified_sql_response = self.llm.complete(simplified_prompt).text.strip()

                            # Remove any code block markers if they exist
                            simplified_sql = re.sub(r'^```\w*\s*', '', simplified_sql_response)
                            simplified_sql = re.sub(r'\s*```$', '', simplified_sql)
                            simplified_sql = simplified_sql.strip()

                            if not simplified_sql.endswith(';'):
                                simplified_sql += ';'

                            print(f"🔍 Executing simplified SQL: {simplified_sql}")
                            results = self.sql_database.run_sql(simplified_sql)

                            # If we successfully got results, return them
                            if results:
                                return self._infer_response(query_str, results, updated_context)
                        except Exception as simple_error:
                            print(f"Simplified query also failed: {str(simple_error)}")
                            pass

                    # If all attempts fail, return a user-friendly error
                    error_message = f"I wasn't able to process that query properly. It seems too complex for me to handle right now. Could you try simplifying or rephrasing your question?"
                    return f"Error: {error_message}"

            # If not always_infer, continue with the original approach
            # Generate SQL
            sql = self._generate_sql(query_context)

            try:
                # Check if SQL is an error message before executing
                if sql and ("SELECT 'No team specified" in sql or "SELECT 'Error" in sql):
                    raise ValueError(f"Predefined SQL returned an error: {sql}")

                # Execute SQL and get results
                if sql:
                    print(f"🔍 Executing SQL: {sql}")
                    results = self.sql_database.run_sql(sql)

                    # Store memory context if needed
                    self.memory_context = query_context

                    # Use LLM to infer and format the response
                    response = self._infer_response(query_str, results, query_context)
                    return response
                else:
                    raise ValueError("No SQL query generated")

            except Exception as sql_error:
                # If predefined patterns fail, try the dynamic inference approach
                print(f"❗ Predefined SQL generation failed: {str(sql_error)}")
                print(f"❗ Using dynamic inference as fallback...")

                if not query_context.get("query_type"):
                    print(f"❗ No clear query type detected, using dynamic inference...")

                # Use dynamic inference to generate SQL
                try:
                    inferred_sql, updated_context = self._infer_query_and_generate_sql(
                        clean_query, query_context
                    )

                    # Check if the inferred SQL is an error message
                    if "SELECT 'Error" in inferred_sql or "SELECT 'Failed" in inferred_sql:
                        raise ValueError(f"Dynamic inference failed: {inferred_sql}")

                    print(f"\n📊 Using dynamically inferred SQL: {inferred_sql}")

                    # Execute the SQL
                    results = self.sql_database.run_sql(inferred_sql)

                    # Store memory context
                    self.memory_context = updated_context

                    # Use LLM to infer and format the response
                    response = self._infer_response(query_str, results, updated_context)
                    return response

                except Exception as infer_error:
                    print(f"❗ Dynamic inference also failed: {str(infer_error)}")
                    traceback.print_exc()
                    # If all approaches fail, return a nice error message
                    return f"I'm sorry, I couldn't understand how to analyze that question. Could you try rephrasing it or asking a different question about the soccer matches?"

        except Exception as e:
            print(f"❗ Unexpected error in query processing: {str(e)}")
            traceback.print_exc()
            return f"I encountered an error while processing your query: {str(e)}. Please try a different question or rephrase your current one."

    def _get_time_period(self, query_str: str) -> tuple[str, str]:
        """
        Get the time period filter from the query using LLM.
        Returns (period_name, sql_filter) tuple.
        """
        # First try simple pattern matching
        query_lower = query_str.lower()
        if "this month" in query_lower or "current month" in query_lower:
            return "month", "date >= DATE_TRUNC('month', CURRENT_DATE) AND date < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'"
        elif "this year" in query_lower or "current year" in query_lower:
            return "year", "date >= DATE_TRUNC('year', CURRENT_DATE)"
        elif "in 2024" in query_lower or "during 2024" in query_lower:
            return "year_2024", "EXTRACT(year FROM date) = 2024"

        # If no simple match, use LLM
        prompt = f"""
        What time period does this query ask for? Analyze the query and return a JSON object with two fields:
        1. "period": a simple name for the period (month/year/year_<year>/none)
        2. "filter": the corresponding SQL filter using DuckDB syntax

        Query: "{query_str}"

        Time Period Guidelines:
        - "this month" or "current month" -> use DATE_TRUNC('month', CURRENT_DATE)
        - "this year" or "current year" -> use DATE_TRUNC('year', CURRENT_DATE)
        - Specific year (e.g. "in 2024") -> use EXTRACT(year FROM date) = <year>
        - No time period mentioned -> return "none" for period and "1=1" for filter

        Example responses:
        For "how did they do this month":
        {{
            "period": "month",
            "filter": "date >= DATE_TRUNC('month', CURRENT_DATE) AND date < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'"
        }}

        For "show me their matches in 2024":
        {{
            "period": "year_2024",
            "filter": "EXTRACT(year FROM date) = 2024"
        }}

        For "what's their record":
        {{
            "period": "none",
            "filter": "1=1"
        }}

        Response (return only the JSON object):"""

        response = self.llm.complete(prompt)

        # Debug output to see the raw response
        print("\n--- Raw LLM Response ---")
        print(f"Type: {type(response)}")
        print(f"Response: {response}")
        if hasattr(response, 'text'):
            print(f"Text type: {type(response.text)}")
            print(f"Text content: {response.text[:500]}...")  # Print first 500 chars
        print("--- End Raw Response ---\n")

        try:
            result = eval(response.text)
            return result["period"], result["filter"]
        except:
            return "none", "1=1"

    def _understand_query(self, query_str: str, memory=None) -> dict:
        """Convert user query to structured representation using LLM."""
        # Convert to lowercase for easier pattern matching
        query_lower = query_str.lower()
        print(f"Processing original query: '{query_str}'")

        # Get time period using focused prompt
        time_period, time_filter = self._get_time_period(query_str)

        # Detect division/league breakdown requests
        division_patterns = [
            r"break.*down by (league|division)",
            r"(league|division) breakdown",
            r"by (league|division)",
            r"(across|per) (league|division)",
            r"for each (league|division)"
        ]

        division_breakdown = False
        for pattern in division_patterns:
            if re.search(pattern, query_lower):
                division_breakdown = True
                break

        # Detect location references like "at NC", "in North Carolina"
        location_patterns = [
            r"at\s+nc\b",
            r"in\s+nc\b",
            r"at\s+north\s+coast\b",
            r"in\s+north\s+coast\b",
            r"north\s+coast",
            r"\bnc\b"
        ]

        location = None
        for pattern in location_patterns:
            if re.search(pattern, query_lower):
                location = "North Coast"
                print(f"Detected location reference: North Coast")
                break

        # Detect formatting requests
        format_patterns = {
            "table": ["make a table", "in table format", "as a table", "in a table", "table format", "tabular format"],
            "chart": ["make a chart", "in chart format", "as a chart", "visualize", "visualization"],
            "summary": ["summarize", "give me a summary", "summary format", "brief overview"],
            "detailed": ["detailed view", "full details", "comprehensive", "all details", "detailed breakdown"],
            "markdown": ["markdown format", "as markdown", "in markdown"]
        }

        detected_format = "default"
        format_explanation = None

        for format_type, patterns in format_patterns.items():
            if any(pattern in query_lower for pattern in patterns):
                detected_format = format_type
                format_explanation = f"Format output as {format_type} as requested"
                break

        # Clean query by removing formatting instructions
        clean_query = query_str
        for format_type, patterns in format_patterns.items():
            for pattern in patterns:
                clean_query = re.sub(f"(?i){pattern}", "", clean_query)
                # Remove common connectors that might be left dangling
                clean_query = re.sub(r"(?i)(and|--|,|\?)\s*$", "", clean_query.strip())

        clean_query = clean_query.strip()
        if clean_query != query_str:
            print(f"Cleaned query (formatting requests removed): '{clean_query}'")
            query_str = clean_query
            query_lower = query_str.lower()

        # Extract year if present
        detected_year = None
        year_match = re.search(r'\b(20\d{2})\b', query_str)
        if year_match:
            detected_year = year_match.group(1)
        elif "this year" in query_lower:
            detected_year = "current"
        elif "last year" in query_lower:
            detected_year = "previous"

        # Add more specific location detection logic if needed
        # This is where you could add state or city-specific patterns

        # At the end of processing, just before returning the final query_context:

        # Add original query and location information to the context
        query_context = {
            "original_query": query_str,
            "location": location,
            "division_breakdown": division_breakdown,
            "format": detected_format
        }

        # Ensure we have valid time period and filter
        if not query_context.get("time_period") or not query_context.get("time_filter"):
            query_context["time_period"] = time_period
            query_context["time_filter"] = time_filter

        # Direct year override based on detection in pre-processing
        if detected_year and not query_context.get("year_filter"):
            query_context["year_filter"] = "2024" if detected_year == "current" else detected_year

        # Add original query and location to the context
        query_context["original_query"] = query_str
        if location:
            query_context["location"] = location

        teams = get_all_teams(self.sql_database._engine)
        team_match = find_best_matching_team(query_lower, teams)
        if team_match:
            original_phrase, matched_team = team_match
            print(f"Matched team name: '{original_phrase}' -> '{matched_team}'")
            query_context["team"] = matched_team
            query_context["original_phrase"] = original_phrase
        elif memory:
            # Check memory for recently mentioned team
            recent_team = memory.get_last_team()
            team_referenced = any(ref in query_lower for ref in ["them", "they", "their", "the team"]) or "break" in query_lower

            # If a division breakdown is requested or team reference is used, try to get team from memory
            if recent_team and (team_referenced or division_breakdown):
                print(f"Using team from memory context: {recent_team}")
                query_context["team"] = recent_team
                query_context["from_memory"] = True

        return query_context

    def _generate_sql(self, query_context: dict) -> str:
        """Generate SQL query based on structured representation."""
        try:
            query_type = query_context.get('query_type', 'stats')
            team_name = query_context.get('team')
            time_filter = query_context.get('time_filter', '1=1')
            limit = query_context.get('limit', 5)
            division_breakdown = query_context.get('division_breakdown', False)

            # If division breakdown is requested, update the query type
            if division_breakdown and team_name:
                query_type = "division_breakdown"
                print(f"Division breakdown requested for team: {team_name}")

            # Detect defensive ranking queries
            query_lower = query_context.get('original_query', '').lower()
            if ('best defense' in query_lower or 'fewest goals conceded' in query_lower) and not team_name:
                query_type = "team_defensive_stats"
                print(f"Detected defensive rankings query: {query_type}")

            # For match listing queries
            if query_type == "match_listing":
                if team_name:
                    # Detailed match listing for a specific team
                    sql = f"""
                    WITH team_matches AS (
                        -- Matches where the team played as home
                        SELECT
                            date,
                            home_team,
                            away_team,
                            home_score,
                            away_score,
                            'home' as venue,
                            CASE
                                WHEN home_score IS NULL OR away_score IS NULL THEN 'N/P'
                                WHEN home_score > away_score THEN 'W'
                                WHEN home_score = away_score THEN 'D'
                                ELSE 'L'
                            END as result
                        FROM matches
                        WHERE REGEXP_REPLACE(home_team, '\\s*\\(\\d+\\)\\s*$', '') = '{team_name}'
                          AND {time_filter}

                        UNION ALL

                        -- Matches where the team played as away
                        SELECT
                            date,
                            home_team,
                            away_team,
                            home_score,
                            away_score,
                            'away' as venue,
                            CASE
                                WHEN home_score IS NULL OR away_score IS NULL THEN 'N/P'
                                WHEN away_score > home_score THEN 'W'
                                WHEN away_score = home_score THEN 'D'
                                ELSE 'L'
                            END as result
                        FROM matches
                        WHERE REGEXP_REPLACE(away_team, '\\s*\\(\\d+\\)\\s*$', '') = '{team_name}'
                          AND {time_filter}
                    )
                    SELECT
                        date,
                        home_team,
                        away_team,
                        home_score,
                        away_score,
                        venue,
                        result
                    FROM team_matches
                    ORDER BY date DESC
                    LIMIT {limit}
                    """
                else:
                    # General match listing without a specific team
                    sql = f"""
                    SELECT
                        date,
                        home_team,
                        away_team,
                        home_score,
                        away_score,
                        home_score || '-' || away_score as score
                    FROM matches
                    WHERE {time_filter}
                    ORDER BY date DESC
                    LIMIT {limit}
                    """
                return sql.strip()

            # For team defensive stats
            elif query_type == "team_defensive_stats":
                sql = f"""
                WITH team_goals_conceded AS (
                    -- Goals conceded as home team
                    SELECT
                        REGEXP_REPLACE(home_team, '\\s*\\(\\d+\\)\\s*$', '') as team,
                        SUM(away_score) as goals_conceded,
                        COUNT(*) as matches_played
                    FROM matches
                    WHERE home_score IS NOT NULL AND away_score IS NOT NULL
                      AND {time_filter}
                    GROUP BY REGEXP_REPLACE(home_team, '\\s*\\(\\d+\\)\\s*$', '')

                    UNION ALL

                    -- Goals conceded as away team
                    SELECT
                        REGEXP_REPLACE(away_team, '\\s*\\(\\d+\\)\\s*$', '') as team,
                        SUM(home_score) as goals_conceded,
                        COUNT(*) as matches_played
                    FROM matches
                    WHERE home_score IS NOT NULL AND away_score IS NOT NULL
                      AND {time_filter}
                    GROUP BY REGEXP_REPLACE(away_team, '\\s*\\(\\d+\\)\\s*$', '')
                )

                SELECT
                    team,
                    SUM(goals_conceded) as total_goals_conceded,
                    SUM(matches_played) as total_matches,
                    ROUND(SUM(goals_conceded) * 1.0 / SUM(matches_played), 2) as avg_goals_conceded_per_match
                FROM team_goals_conceded
                GROUP BY team
                HAVING SUM(matches_played) >= 5 -- Minimum number of matches to be considered
                ORDER BY avg_goals_conceded_per_match ASC, total_goals_conceded ASC
                LIMIT 20
                """
                query_context['metrics'] = ['team', 'total_goals_conceded', 'total_matches', 'avg_goals_conceded_per_match']
                query_context['explanation'] = "Teams ranked by best defense (lowest average goals conceded per match)"
                return sql.strip()

            # For highest scoring games
            if query_type == "highest_scoring_games":
                sql = f"""
                SELECT
                    date,
                    home_team,
                    away_team,
                    home_score,
                    away_score,
                    home_score + away_score as total_goals
                FROM matches
                WHERE {time_filter}
                ORDER BY total_goals DESC, date DESC
                LIMIT {limit}
                """
                return sql.strip()

            # For team rankings
            if query_type == "team_rankings":
                ranking_metric = query_context.get("ranking_metric", "matches_played")
                limit = query_context.get("limit", 10)

                # Base CTE to count home and away matches for all teams
                sql = f"""
                WITH team_matches AS (
                    -- Count matches where team played as home
                    SELECT
                        REGEXP_REPLACE(home_team, '\\s*\\(\\d+\\)\\s*$', '') as team_name,
                        date,
                        home_score as goals_for,
                        away_score as goals_against,
                        CASE
                            WHEN home_score IS NULL OR away_score IS NULL THEN 'N/P'
                            WHEN home_score > away_score THEN 'W'
                            WHEN home_score = away_score THEN 'D'
                            ELSE 'L'
                        END as result
                    FROM matches
                    WHERE {time_filter}

                    UNION ALL

                    -- Count matches where team played as away
                    SELECT
                        REGEXP_REPLACE(away_team, '\\s*\\(\\d+\\)\\s*$', '') as team_name,
                        date,
                        away_score as goals_for,
                        home_score as goals_against,
                        CASE
                            WHEN home_score IS NULL OR away_score IS NULL THEN 'N/P'
                            WHEN away_score > home_score THEN 'W'
                            WHEN away_score = home_score THEN 'D'
                            ELSE 'L'
                        END as result
                    FROM matches
                    WHERE {time_filter}
                ),

                team_stats AS (
                    SELECT
                        team_name,
                        COUNT(*) as matches_played,
                        COUNT(CASE WHEN result = 'W' THEN 1 END) as wins,
                        COUNT(CASE WHEN result = 'D' THEN 1 END) as draws,
                        COUNT(CASE WHEN result = 'L' THEN 1 END) as losses,
                        COUNT(CASE WHEN result = 'N/P' THEN 1 END) as not_played,
                        SUM(CASE WHEN goals_for IS NOT NULL THEN goals_for ELSE 0 END) as goals_scored,
                        SUM(CASE WHEN goals_against IS NOT NULL THEN goals_against ELSE 0 END) as goals_conceded,
                        SUM(CASE WHEN goals_for IS NOT NULL AND goals_against IS NOT NULL THEN goals_for - goals_against ELSE 0 END) as goal_difference,
                        ROUND(100.0 * COUNT(CASE WHEN result = 'W' THEN 1 END) / NULLIF(COUNT(CASE WHEN result IN ('W', 'D', 'L') THEN 1 END), 1) as win_percentage
                    FROM team_matches
                    GROUP BY team_name
                    HAVING COUNT(*) > 0
                )

                SELECT
                    team_name,
                    matches_played,
                    wins,
                    draws,
                    losses,
                    goals_scored,
                    goals_conceded,
                    goal_difference,
                    win_percentage
                FROM team_stats
                """

                # Add appropriate ORDER BY clause based on ranking metric
                if ranking_metric == "matches_played":
                    sql += f"ORDER BY matches_played DESC, wins DESC, goal_difference DESC"
                elif ranking_metric == "goals_scored":
                    sql += f"ORDER BY goals_scored DESC, matches_played DESC, wins DESC"
                elif ranking_metric == "wins":
                    sql += f"ORDER BY wins DESC, matches_played DESC, goal_difference DESC"
                elif ranking_metric == "win_percentage":
                    sql += f"ORDER BY win_percentage DESC, matches_played DESC, goal_difference DESC"
                else:
                    # Default sorting by matches played
                    sql += f"ORDER BY matches_played DESC, wins DESC, goal_difference DESC"

                sql += f"\nLIMIT {limit};"

                return sql.strip()

            # For aggregate statistics across all teams (no specific team)
            elif query_type == "aggregate_stats":
                sql = f"""
                SELECT
                    COUNT(*) as total_matches,
                    COUNT(DISTINCT date) as days_with_matches,
                    ROUND(AVG(home_score + away_score), 2) as avg_goals_per_match,
                    SUM(home_score + away_score) as total_goals,
                    MAX(home_score + away_score) as highest_scoring_match,
                    COUNT(DISTINCT home_team) + COUNT(DISTINCT away_team) as teams_played
                FROM matches
                WHERE {time_filter}
                """
                return sql.strip()

            # For daily statistics / day-by-day match counts
            elif query_type == 'daily_stats':
                sql = f"""
                SELECT
                    date,
                    COUNT(*) as matches_count,
                    COUNT(DISTINCT home_team) + COUNT(DISTINCT away_team) as teams_count,
                    SUM(home_score + away_score) as total_goals,
                    ROUND(AVG(home_score + away_score), 2) as avg_goals_per_match
                FROM matches
                WHERE {time_filter}
                GROUP BY date
                ORDER BY matches_count DESC, date DESC
                LIMIT {limit};
                """
                return sql.strip()

            # For hardest opponent analysis
            elif query_type == 'hardest_opponent':
                # Check if we have a team name
                if not team_name:
                    return "SELECT 'No team specified for hardest opponent analysis' as error"

                sql = f"""
                WITH team_matches AS (
                    -- Matches where the team played as home
                    SELECT
                        date,
                        REGEXP_REPLACE(home_team, '\\s*\\(\\d+\\)\\s*$', '') as team,
                        REGEXP_REPLACE(away_team, '\\s*\\(\\d+\\)\\s*$', '') as opponent,
                        home_score as goals_for,
                        away_score as goals_against,
                        CASE WHEN home_score IS NOT NULL AND away_score IS NOT NULL THEN home_score - away_score ELSE NULL END as score_margin,
                        CASE
                            WHEN home_score IS NULL OR away_score IS NULL THEN 'N/P'
                            WHEN home_score > away_score THEN 'W'
                            WHEN home_score = away_score THEN 'D'
                            ELSE 'L'
                        END as result
                    FROM matches
                    WHERE REGEXP_REPLACE(home_team, '\\s*\\(\\d+\\)\\s*$', '') = '{team_name}'
                      AND {time_filter}

                    UNION ALL

                    -- Matches where the team played as away
                    SELECT
                        date,
                        REGEXP_REPLACE(away_team, '\\s*\\(\\d+\\)\\s*$', '') as team,
                        REGEXP_REPLACE(home_team, '\\s*\\(\\d+\\)\\s*$', '') as opponent,
                        away_score as goals_for,
                        home_score as goals_against,
                        CASE WHEN away_score IS NOT NULL AND home_score IS NOT NULL THEN away_score - home_score ELSE NULL END as score_margin,
                        CASE
                            WHEN home_score IS NULL OR away_score IS NULL THEN 'N/P'
                            WHEN away_score > home_score THEN 'W'
                            WHEN away_score = home_score THEN 'D'
                            ELSE 'L'
                        END as result
                    FROM matches
                    WHERE REGEXP_REPLACE(away_team, '\\s*\\(\\d+\\)\\s*$', '') = '{team_name}'
                      AND {time_filter}
                ),
                opponent_stats AS (
                    SELECT
                        opponent,
                        COUNT(*) as games_played,
                        COUNT(CASE WHEN result = 'W' THEN 1 END) as wins,
                        COUNT(CASE WHEN result = 'D' THEN 1 END) as draws,
                        COUNT(CASE WHEN result = 'L' THEN 1 END) as losses,
                        COUNT(CASE WHEN result = 'N/P' THEN 1 END) as not_played,
                        SUM(CASE WHEN goals_for IS NOT NULL THEN goals_for ELSE 0 END) as goals_scored,
                        SUM(CASE WHEN goals_against IS NOT NULL THEN goals_against ELSE 0 END) as goals_conceded,
                        SUM(CASE WHEN goals_for IS NOT NULL AND goals_against IS NOT NULL THEN goals_against - goals_for ELSE 0 END) as goal_difference,
                        AVG(CASE WHEN score_margin IS NOT NULL THEN score_margin ELSE NULL END) as avg_margin,
                        ROUND(100.0 * COUNT(CASE WHEN result = 'L' THEN 1 END) /
                              NULLIF(COUNT(CASE WHEN result IN ('W', 'D', 'L') THEN 1 END), 1) as loss_percentage
                    FROM team_matches
                    GROUP BY opponent
                    HAVING COUNT(*) > 0  -- Ensure at least one match played
                )
                SELECT
                    opponent as hardest_opponent,
                    games_played,
                    wins,
                    draws,
                    losses,
                    goals_scored,
                    goals_conceded,
                    goal_difference,
                    avg_margin,
                    loss_percentage
                FROM opponent_stats
                WHERE games_played > 0  -- Extra check to ensure valid data
                ORDER BY
                    -- Prioritize higher loss percentage
                    loss_percentage DESC,
                    -- Then by goal difference (more negative is worse)
                    goal_difference DESC,
                    -- Then by average margin (more negative is worse)
                    avg_margin DESC,
                    -- Then by games played (more games means more significant opponent)
                    games_played DESC
                LIMIT 5;
                """
                return sql.strip()

            # For basic team statistics
            elif query_type == 'stats':
                # Check if we have a team name
                if not team_name:
                    return "SELECT 'No team specified for statistics analysis' as error"

                sql = f"""
                WITH team_matches AS (
                    -- Matches where the team played as home
                    SELECT
                        date,
                        REGEXP_REPLACE(home_team, '\\s*\\(\\d+\\)\\s*$', '') as team,
                        REGEXP_REPLACE(away_team, '\\s*\\(\\d+\\)\\s*$', '') as opponent,
                        home_score as goals_for,
                        away_score as goals_against,
                        CASE
                            WHEN home_score IS NULL OR away_score IS NULL THEN 'N/P'
                            WHEN home_score > away_score THEN 'W'
                            WHEN home_score = away_score THEN 'D'
                            ELSE 'L'
                        END as result
                    FROM matches
                    WHERE REGEXP_REPLACE(home_team, '\\s*\\(\\d+\\)\\s*$', '') = '{team_name}'
                      AND {time_filter}

                    UNION ALL

                    -- Matches where the team played as away
                    SELECT
                        date,
                        REGEXP_REPLACE(away_team, '\\s*\\(\\d+\\)\\s*$', '') as team,
                        REGEXP_REPLACE(home_team, '\\s*\\(\\d+\\)\\s*$', '') as opponent,
                        away_score as goals_for,
                        home_score as goals_against,
                        CASE
                            WHEN home_score IS NULL OR away_score IS NULL THEN 'N/P'
                            WHEN away_score > home_score THEN 'W'
                            WHEN away_score = home_score THEN 'D'
                            ELSE 'L'
                        END as result
                    FROM matches
                    WHERE REGEXP_REPLACE(away_team, '\\s*\\(\\d+\\)\\s*$', '') = '{team_name}'
                      AND {time_filter}
                )
                SELECT
                    COUNT(*) as total_matches,
                    COUNT(CASE WHEN result = 'W' THEN 1 END) as wins,
                    COUNT(CASE WHEN result = 'D' THEN 1 END) as draws,
                    COUNT(CASE WHEN result = 'L' THEN 1 END) as losses,
                    COUNT(CASE WHEN result = 'N/P' THEN 1 END) as not_played,
                    SUM(CASE WHEN goals_for IS NOT NULL THEN goals_for ELSE 0 END) as total_goals_scored,
                    SUM(CASE WHEN goals_against IS NOT NULL THEN goals_against ELSE 0 END) as total_goals_conceded,
                    SUM(CASE WHEN goals_for IS NOT NULL AND goals_against IS NOT NULL THEN goals_for - goals_against ELSE 0 END) as goal_difference,
                    ROUND(100.0 * COUNT(CASE WHEN result = 'W' THEN 1 END) /
                          NULLIF(COUNT(CASE WHEN result IN ('W', 'D', 'L') THEN 1 END), 0), 1) as win_percentage
                FROM team_matches
                """
                return sql.strip()

            # Division breakdown query - shows team performance by division
            elif query_type == "division_breakdown" and team_name:
                # Get statistics for the team broken down by division
                sql = f"""
                WITH team_matches AS (
                    -- Matches where the team played as home
                    SELECT
                        date,
                        home_team,
                        away_team,
                        home_score,
                        away_score,
                        'home' as venue,
                        CASE
                            WHEN home_score IS NULL OR away_score IS NULL THEN 'N/P'
                            WHEN home_score > away_score THEN 'W'
                            WHEN home_score = away_score THEN 'D'
                            ELSE 'L'
                        END as result,
                        CASE
                            WHEN home_team LIKE '%(%' THEN
                                SUBSTRING(home_team FROM POSITION('(' IN home_team) + 1 FOR POSITION(')' IN home_team) - POSITION('(' IN home_team) - 1)
                            ELSE NULL
                        END as division
                    FROM matches
                    WHERE home_team LIKE '{team_name}%'
                      AND {time_filter}

                    UNION ALL

                    -- Matches where the team played as away
                    SELECT
                        date,
                        home_team,
                        away_team,
                        home_score,
                        away_score,
                        'away' as venue,
                        CASE
                            WHEN home_score IS NULL OR away_score IS NULL THEN 'N/P'
                            WHEN home_score < away_score THEN 'W'
                            WHEN home_score = away_score THEN 'D'
                            ELSE 'L'
                        END as result,
                        CASE
                            WHEN away_team LIKE '%(%' THEN
                                SUBSTRING(away_team FROM POSITION('(' IN away_team) + 1 FOR POSITION(')' IN away_team) - POSITION('(' IN away_team) - 1)
                            ELSE NULL
                        END as division
                    FROM matches
                    WHERE away_team LIKE '{team_name}%'
                      AND {time_filter}
                )

                SELECT
                    CASE
                        WHEN division IS NULL THEN 'No Division'
                        ELSE CONCAT('{team_name} (', division, ')')
                    END as team_name,
                    COUNT(*) as matches_played,
                    SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN result = 'D' THEN 1 ELSE 0 END) as draws,
                    SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
                    SUM(CASE WHEN result = 'N/P' THEN 1 ELSE 0 END) as not_played,
                    ROUND(
                        CASE
                            WHEN COUNT(CASE WHEN result != 'N/P' THEN 1 END) = 0 THEN 0
                            ELSE (SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) * 100.0 /
                                 NULLIF(COUNT(CASE WHEN result != 'N/P' THEN 1 END), 0))
                        END,
                        1
                    ) as win_percentage
                FROM team_matches
                GROUP BY division
                ORDER BY matches_played DESC, win_percentage DESC
                LIMIT {limit};
                """
                print(f"Generated division breakdown SQL for team {team_name}")
                query_context['metrics'] = ['matches_played', 'wins', 'draws', 'losses', 'win_percentage']
                query_context['explanation'] = f"Team statistics for {team_name} broken down by division"

            # Fallback to a simple stats query for unhandled query types
            else:
                if team_name:
                    # Simple team match listing if no other query type matched
                    sql = f"""
                    WITH team_matches AS (
                        -- Matches where the team played as home
                        SELECT
                            date,
                            REGEXP_REPLACE(home_team, '\\s*\\(\\d+\\)\\s*$', '') as team,
                            REGEXP_REPLACE(away_team, '\\s*\\(\\d+\\)\\s*$', '') as opponent,
                            home_score as goals_for,
                            away_score as goals_against,
                            'home' as venue,
                            CASE
                                WHEN home_score IS NULL OR away_score IS NULL THEN 'N/P'
                                WHEN home_score > away_score THEN 'W'
                                WHEN home_score = away_score THEN 'D'
                                ELSE 'L'
                            END as result
                        FROM matches
                        WHERE REGEXP_REPLACE(home_team, '\\s*\\(\\d+\\)\\s*$', '') = '{team_name}'
                          AND {time_filter}

                        UNION ALL

                        -- Matches where the team played as away
                        SELECT
                            date,
                            REGEXP_REPLACE(away_team, '\\s*\\(\\d+\\)\\s*$', '') as team,
                            REGEXP_REPLACE(home_team, '\\s*\\(\\d+\\)\\s*$', '') as opponent,
                            away_score as goals_for,
                            home_score as goals_against,
                            'away' as venue,
                            CASE
                                WHEN home_score IS NULL OR away_score IS NULL THEN 'N/P'
                                WHEN away_score > home_score THEN 'W'
                                WHEN away_score = home_score THEN 'D'
                                ELSE 'L'
                            END as result
                        FROM matches
                        WHERE REGEXP_REPLACE(away_team, '\\s*\\(\\d+\\)\\s*$', '') = '{team_name}'
                          AND {time_filter}
                    )
                    SELECT
                        date,
                        team || ' vs ' || opponent as matchup,
                        goals_for,
                        goals_against,
                        venue,
                        result
                    FROM team_matches
                    ORDER BY date DESC
                    LIMIT {limit}
                    """
                    return sql.strip()
                else:
                    return """
                    SELECT
                        COUNT(*) as total_matches,
                        COUNT(DISTINCT date) as unique_dates,
                        COUNT(DISTINCT home_team) + COUNT(DISTINCT away_team) as total_teams,
                        SUM(home_score + away_score) as total_goals,
                        ROUND(AVG(home_score + away_score), 2) as avg_goals_per_match
                    FROM matches
                    """

        except Exception as e:
            print(f"\nError generating SQL: {e}")
            return "SELECT 'Error generating SQL' as error"

    def _infer_query_and_generate_sql(self, query_str: str, query_context: dict) -> tuple:
        """
        Dynamically infer what is being asked and generate SQL based on schema analysis.
        This is used as a fallback when predefined query patterns don't match.

        Returns a tuple of (sql_query, updated_query_context)
        """
        print("\n🔍 Using dynamic query inference...")

        # Create a comprehensive prompt that describes the database schema
        # and asks the LLM to generate appropriate SQL
        schema_description = self._get_table_context()

        # Add any detected context we already have
        context_info = []
        if query_context.get('team'):
            context_info.append(f"- Team mentioned: {query_context['team']}")
        if query_context.get('time_period'):
            context_info.append(f"- Time period: {query_context['time_period']}")
        if query_context.get('time_filter'):
            context_info.append(f"- Time filter: {query_context['time_filter']}")
        if query_context.get('location'):
            context_info.append(f"- Location: {query_context['location']}")

        # Check if division is mentioned in the query
        division_mentioned = False
        division_letter = None
        division_pattern = r'division\s+([a-z])\b'
        division_match = re.search(division_pattern, query_str.lower())
        if division_match:
            division_letter = division_match.group(1).upper()
            division_mentioned = True
            context_info.append(f"- Division mentioned: Division {division_letter}")

        # Prepare a more detailed example SQL queries section to guide the model
        example_sql_queries = """
        Example SQL queries for different scenarios:

        1. Team win record:
        WITH team_matches AS (
            -- Matches where the team played as home
            SELECT
                date,
                REGEXP_REPLACE(home_team, '\\s*\\(\\d+\\)\\s*$', '') as team,
                REGEXP_REPLACE(away_team, '\\s*\\(\\d+\\)\\s*$', '') as opponent,
                home_score as goals_for,
                away_score as goals_against,
                CASE
                    WHEN home_score IS NULL OR away_score IS NULL THEN 'N/P'
                    WHEN home_score > away_score THEN 'W'
                    WHEN home_score = away_score THEN 'D'
                    ELSE 'L'
                END as result
            FROM matches
            WHERE REGEXP_REPLACE(home_team, '\\s*\\(\\d+\\)\\s*$', '') = 'Team Name'
              AND date >= DATE_TRUNC('year', CURRENT_DATE)

            UNION ALL

            -- Matches where the team played as away
            SELECT
                date,
                REGEXP_REPLACE(away_team, '\\s*\\(\\d+\\)\\s*$', '') as team,
                REGEXP_REPLACE(home_team, '\\s*\\(\\d+\\)\\s*$', '') as opponent,
                away_score as goals_for,
                home_score as goals_against,
                CASE
                    WHEN home_score IS NULL OR away_score IS NULL THEN 'N/P'
                    WHEN away_score > home_score THEN 'W'
                    WHEN away_score = home_score THEN 'D'
                    ELSE 'L'
                END as result
            FROM matches
            WHERE REGEXP_REPLACE(away_team, '\\s*\\(\\d+\\)\\s*$', '') = 'Team Name'
              AND date >= DATE_TRUNC('year', CURRENT_DATE)
        )
        SELECT
            COUNT(*) as total_matches,
            COUNT(CASE WHEN result = 'W' THEN 1 END) as wins,
            COUNT(CASE WHEN result = 'D' THEN 1 END) as draws,
            COUNT(CASE WHEN result = 'L' THEN 1 END) as losses,
            COUNT(CASE WHEN result = 'N/P' THEN 1 END) as not_played,
            ROUND(100.0 * COUNT(CASE WHEN result = 'W' THEN 1 END) /
                NULLIF(COUNT(CASE WHEN result IN ('W', 'D', 'L') THEN 1 END), 0), 1) as win_percentage
        FROM team_matches

        2. Best teams in a specific division (e.g., Division C):
        WITH team_stats AS (
            -- Home matches
            SELECT
                REGEXP_REPLACE(home_team, '\\s*\\([^)]*\\)\\s*$', '') as team_name,
                -- Extract division from team name
                CASE
                    WHEN home_team LIKE '%(C)%' OR home_team LIKE '%(c)%' THEN 'C'
                    WHEN home_team LIKE '%(A)%' OR home_team LIKE '%(a)%' THEN 'A'
                    WHEN home_team LIKE '%(B)%' OR home_team LIKE '%(b)%' THEN 'B'
                    WHEN home_team LIKE '%(D)%' OR home_team LIKE '%(d)%' THEN 'D'
                    WHEN home_team LIKE '%(%' THEN SUBSTRING(home_team FROM POSITION('(' IN home_team) + 1 FOR 1)
                    ELSE NULL
                END as division,
                CASE
                    WHEN home_score > away_score THEN 3
                    WHEN home_score = away_score THEN 1
                    ELSE 0
                END as points,
                CASE
                    WHEN home_score > away_score THEN 1
                    ELSE 0
                END as wins,
                CASE
                    WHEN home_score = away_score THEN 1
                    ELSE 0
                END as draws,
                CASE
                    WHEN home_score < away_score THEN 1
                    ELSE 0
                END as losses,
                home_score as goals_for,
                away_score as goals_against,
                1 as matches_played
            FROM matches
            WHERE date >= DATE_TRUNC('month', CURRENT_DATE)
              AND date < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'
              AND home_score IS NOT NULL AND away_score IS NOT NULL

            UNION ALL

            -- Away matches
            SELECT
                REGEXP_REPLACE(away_team, '\\s*\\([^)]*\\)\\s*$', '') as team_name,
                -- Extract division from team name
                CASE
                    WHEN away_team LIKE '%(C)%' OR away_team LIKE '%(c)%' THEN 'C'
                    WHEN away_team LIKE '%(A)%' OR away_team LIKE '%(a)%' THEN 'A'
                    WHEN away_team LIKE '%(B)%' OR away_team LIKE '%(b)%' THEN 'B'
                    WHEN away_team LIKE '%(D)%' OR away_team LIKE '%(d)%' THEN 'D'
                    WHEN away_team LIKE '%(%' THEN SUBSTRING(away_team FROM POSITION('(' IN away_team) + 1 FOR 1)
                    ELSE NULL
                END as division,
                CASE
                    WHEN away_score > home_score THEN 3
                    WHEN away_score = home_score THEN 1
                    ELSE 0
                END as points,
                CASE
                    WHEN away_score > home_score THEN 1
                    ELSE 0
                END as wins,
                CASE
                    WHEN away_score = home_score THEN 1
                    ELSE 0
                END as draws,
                CASE
                    WHEN away_score < home_score THEN 1
                    ELSE 0
                END as losses,
                away_score as goals_for,
                home_score as goals_against,
                1 as matches_played
            FROM matches
            WHERE date >= DATE_TRUNC('month', CURRENT_DATE)
              AND date < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'
              AND home_score IS NOT NULL AND away_score IS NOT NULL
        )

        SELECT
            team_name,
            division,
            SUM(points) as total_points,
            SUM(wins) as wins,
            SUM(draws) as draws,
            SUM(losses) as losses,
            SUM(goals_for) as goals_for,
            SUM(goals_against) as goals_against,
            SUM(goals_for) - SUM(goals_against) as goal_difference,
            SUM(matches_played) as matches_played,
            ROUND(SUM(points) * 1.0 / SUM(matches_played), 2) as points_per_game
        FROM team_stats
        WHERE division = 'C'
        GROUP BY team_name, division
        HAVING SUM(matches_played) > 0
        ORDER BY total_points DESC, goal_difference DESC
        LIMIT 10;

        3. Best defensive teams:
        WITH team_goals_conceded AS (
            -- Goals conceded as home team
            SELECT
                REGEXP_REPLACE(home_team, '\\s*\\(\\d+\\)\\s*$', '') as team,
                SUM(away_score) as goals_conceded,
                COUNT(*) as matches_played
            FROM matches
            WHERE home_score IS NOT NULL AND away_score IS NOT NULL
              AND 1=1
            GROUP BY REGEXP_REPLACE(home_team, '\\s*\\(\\d+\\)\\s*$', '')

            UNION ALL

            -- Goals conceded as away team
            SELECT
                REGEXP_REPLACE(away_team, '\\s*\\(\\d+\\)\\s*$', '') as team,
                SUM(home_score) as goals_conceded,
                COUNT(*) as matches_played
            FROM matches
            WHERE home_score IS NOT NULL AND away_score IS NOT NULL
              AND 1=1
            GROUP BY REGEXP_REPLACE(away_team, '\\s*\\(\\d+\\)\\s*$', '')
        )

        SELECT
            team,
            SUM(goals_conceded) as total_goals_conceded,
            SUM(matches_played) as total_matches,
            ROUND(SUM(goals_conceded) * 1.0 / SUM(matches_played), 2) as avg_goals_conceded_per_match
        FROM team_goals_conceded
        GROUP BY team
        HAVING SUM(matches_played) >= 5 -- Minimum number of matches to be considered
        ORDER BY avg_goals_conceded_per_match ASC, total_goals_conceded ASC
        LIMIT 20;
        """

        # Add division-specific guidance if relevant
        division_guidance = ""
        if division_mentioned:
            division_guidance = f"""
            DIVISION GUIDANCE:
            - The user is asking about Division {division_letter}
            - In the database, divisions are typically indicated in parentheses in team names, e.g., "Team Name (C)"
            - When filtering for Division {division_letter}, use a pattern like:
              `WHERE home_team LIKE '%({division_letter})%' OR away_team LIKE '%({division_letter})%'`
            - You can also extract the division by checking if the character in parentheses matches '{division_letter}'
            - Always include the division letter in your response
            """

        # Build the prompt for the LLM
        prompt = f"""
        I need you to analyze the following user question and generate appropriate SQL for our soccer matches database:

        USER QUESTION: "{query_str}"

        DATABASE SCHEMA:
        {schema_description}

        ALREADY DETECTED CONTEXT:
        {chr(10).join(context_info) if context_info else "No specific context detected yet."}

        {division_guidance}

        {example_sql_queries}

        IMPORTANT NOTES:
        1. Team names in the database may include division numbers in parentheses, e.g. "Team Name (2)" or division letters e.g. "Team Name (C)"
        2. When matching team names, use REGEXP_REPLACE to remove division indicators: REGEXP_REPLACE(team_name, '\\s*\\([^)]*\\)\\s*$', '')
        3. For calculating win percentages, use: ROUND(100.0 * wins / NULLIF(total_played_matches, 0), 1)
        4. For time filters, use the appropriate DuckDB date functions from the schema description
        5. For queries about "best" teams, rank by points (win=3, draw=1, loss=0) with a reasonable minimum number of matches played
        6. If the user mentions a division (like "Division C"), look for teams with that letter in parentheses
        7. If no results would be returned, provide a GROUP BY fallback that shows aggregate stats instead
        8. ENSURE YOUR SQL QUERY IS COMPLETE. It must end with a semicolon and include all closing parentheses.

        QUERY STRATEGY:
        1. If the user asks about a specific division, first filter for teams in that division
        2. If the user asks about "best" teams, sort by points (then goal difference as a tiebreaker)
        3. If a team or division might not exist, include a GROUP BY to aggregate data rather than returning nothing
        4. For time-specific queries, honor the time filter in the context if available

        INSTRUCTIONS:
        1. First, understand what the user is asking for
        2. Generate a valid SQL query for DuckDB that answers the question
        3. ONLY return the SQL query as plain text
        4. DO NOT include any explanations, JSON formatting, markdown code blocks, or comments
        5. The response should start with SELECT or WITH and contain only valid SQL
        6. DOUBLE CHECK that the SQL query is complete before returning
        """

        # Get the response from Claude
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                response = self.llm.complete(prompt)
                # Print the raw response for debugging
                print(f"\n--- Raw LLM Response ---")
                print(f"Type: {type(response)}")
                print(f"Response: {response.text[:500]}")
                print(f"Text type: {type(response.text)}")
                print(f"Text content: {response.text[:500]}...")
                print(f"--- End Raw Response ---\n")

                # Check if the response seems to be valid SQL
                sql = response.text.strip()

                # Basic validation - check for semicolon at the end and balanced parentheses
                if not sql.endswith(';'):
                    sql = sql + ';'

                # Count opening and closing parentheses
                open_parens = sql.count('(')
                close_parens = sql.count(')')

                # Check for incomplete CASE statements
                case_starts = len(re.findall(r'CASE\s+WHEN', sql, re.IGNORECASE))
                case_ends = len(re.findall(r'END\s+as', sql, re.IGNORECASE)) + len(re.findall(r'END\s*,', sql, re.IGNORECASE)) + len(re.findall(r'END\s*$', sql, re.IGNORECASE)) + len(re.findall(r'END\s+[a-zA-Z]', sql, re.IGNORECASE))

                # Check for incomplete CASE statements and truncated SQL
                if case_starts > case_ends or "CASE" in sql[-30:].upper() or "WHEN" in sql[-30:].upper():
                    if attempt < max_attempts - 1:
                        print(f"⚠️ Generated SQL has incomplete CASE statements. Retrying... (Attempt {attempt+1}/{max_attempts})")
                        prompt += "\n\nThe previous response had incomplete CASE statements. Please ensure all CASE statements have END clauses."
                        continue

                if open_parens != close_parens:
                    if attempt < max_attempts - 1:
                        print(f"⚠️ Generated SQL has unbalanced parentheses. Retrying... (Attempt {attempt+1}/{max_attempts})")
                        # Add more specific instructions on the retry
                        prompt += f"\n\nThe previous response had {open_parens} opening parentheses but {close_parens} closing parentheses. Please fix this and ensure the query is complete."
                        continue
                    else:
                        # On the last attempt, try to fix it automatically
                        missing_parens = open_parens - close_parens
                        if missing_parens > 0:
                            sql = sql[:-1] + (')' * missing_parens) + ';'
                            print(f"⚠️ Automatically added {missing_parens} closing parentheses to balance the query.")

                # Ensure the SQL has proper division filtering if division is mentioned in the query
                if division_mentioned and division_letter and "division" in sql.lower():
                    # Check if the division is properly filtered
                    division_patterns = [
                        f"division\\s*=\\s*'{division_letter}'",
                        f"division\\s*=\\s*\"{division_letter}\"",
                        f"home_team\\s+LIKE\\s+'%\\({division_letter}\\)%'",
                        f"away_team\\s+LIKE\\s+'%\\({division_letter}\\)%'"
                    ]
                    has_proper_division_filter = any(re.search(pattern, sql, re.IGNORECASE) for pattern in division_patterns)

                    if not has_proper_division_filter and attempt < max_attempts - 1:
                        print(f"⚠️ Generated SQL does not properly filter for Division {division_letter}. Retrying...")
                        prompt += f"\n\nThe previous response did not properly filter for Division {division_letter}. Please ensure you add filters for teams in that division."
                        continue

                # Remove any trailing incomplete statements
                if sql.rstrip(');').rstrip().endswith(','):
                    # Find the last complete SELECT statement or CTE
                    match = re.search(r'(WITH|SELECT).*?FROM.*?(WHERE|GROUP\s+BY|ORDER\s+BY|LIMIT|$)', sql, re.IGNORECASE | re.DOTALL)
                    if match and match.end() < len(sql) - 20:
                        # Truncate the SQL to just the complete parts
                        sql = sql[:match.end()] + ';'
                        print("⚠️ Truncated SQL to remove incomplete trailing statements.")

                # Check for incomplete CTEs by looking for ")" followed by SELECT but not preceded by FROM
                if re.search(r'\)\s+SELECT', sql) and not re.search(r'FROM\s+\w+\s*\)\s+SELECT', sql):
                    print("⚠️ Warning: SQL may have incomplete CTEs. Attempting to simplify the query.")
                    # Try to generate a simplified query
                    if attempt < max_attempts - 1:
                        prompt += "\n\nPlease generate a simpler SQL query with fewer CTEs and joins."
                        continue

                # Check for incomplete UNION ALL sections
                if "UNION ALL" in sql and "WHERE" in sql:
                    # Check for incomplete WHERE clauses after UNION ALL
                    union_parts = sql.split("UNION ALL")
                    for i, part in enumerate(union_parts[1:], 1):  # Skip the first part before UNION ALL
                        # Check if WHERE clause is incomplete
                        if "WHERE" in part and ")" in part and part.rfind("WHERE") > part.rfind(")"):
                            where_pos = part.rfind("WHERE")
                            incomplete_where = part[where_pos:]
                            # Check for incomplete DATE_TRUNC expressions
                            if "DATE_TRUNC" in incomplete_where and "'" in incomplete_where:
                                if incomplete_where.count("'") % 2 != 0:  # Odd number of quotes
                                    # Fix the incomplete DATE_TRUNC clause
                                    if attempt < max_attempts - 1:
                                        print(f"⚠️ Found incomplete DATE_TRUNC in WHERE clause after UNION ALL. Retrying... (Attempt {attempt+1}/{max_attempts})")
                                        prompt += "\n\nThe previous response had an incomplete DATE_TRUNC function in a WHERE clause after UNION ALL. Make sure all string literals are properly closed with quotes."
                                        continue
                                    else:
                                        # On last attempt, try to fix it
                                        last_quote_pos = incomplete_where.rfind("'")
                                        fixed_part = incomplete_where[:last_quote_pos+1] + ", CURRENT_DATE) AND date < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'"
                                        union_parts[i] = part.replace(incomplete_where, fixed_part)
                                        sql = "UNION ALL".join(union_parts)
                                        print("⚠️ Automatically fixed incomplete DATE_TRUNC in WHERE clause.")

                # Ensure there's a group by fallback for queries about divisions or teams that might not exist
                if ("division" in sql.lower() or "team" in sql.lower()) and not "GROUP BY" in sql.upper():
                    # For the last attempt, try to add a simple GROUP BY if the query is for teams or divisions
                    if attempt == max_attempts - 1 and (division_mentioned or "team" in query_str.lower()):
                        print("⚠️ Adding GROUP BY fallback to ensure we get results even if specific entities aren't found")
                        # Try to simplify to an aggregate query that will return results
                        try:
                            # Create a simpler prompt for SQL generation with explicit GROUP BY instructions
                            group_by_prompt = f"""
                            Create a SQL query for this question: "{query_str}"
                            that uses GROUP BY to show aggregate statistics by team or division.

                            GOAL: Ensure the query returns useful data even if specific teams/divisions aren't found.

                            DATABASE SCHEMA:
                            {schema_description}

                            TIME FILTER: {query_context.get('time_filter', '1=1')}

                            IMPORTANT:
                            - Use only basic SQL constructs
                            - Include a GROUP BY clause
                            - Sort by points or matches played
                            - ONLY return the SQL query with no explanations
                            """

                            # Get a simplified GROUP BY query
                            group_by_response = self.llm.complete(group_by_prompt)
                            group_by_sql = group_by_response.text.strip()

                            # Basic validation
                            if "GROUP BY" in group_by_sql.upper() and ("SELECT" in group_by_sql.upper() or "WITH" in group_by_sql.upper()):
                                sql = group_by_sql
                                if not sql.endswith(';'):
                                    sql += ';'
                                print("✅ Using GROUP BY fallback query to ensure results")
                        except Exception as group_by_error:
                            print(f"Error generating GROUP BY fallback: {str(group_by_error)}")

                # Success - return the SQL and update context
                # Copy and update query context for the memory
                updated_context = query_context.copy()
                updated_context['original_query'] = query_str
                updated_context['inferred_sql'] = True
                if division_mentioned:
                    updated_context['division'] = division_letter

                # Debug the final SQL
                print(f"\n🔍 Final generated SQL:\n{sql}")

                return sql, updated_context

            except Exception as e:
                print(f"Error in LLM query generation (Attempt {attempt+1}/{max_attempts}): {str(e)}")
                if attempt < max_attempts - 1:
                    print("Retrying with simplified prompt...")
                    # Simplify the prompt for retry
                    prompt = f"""
                    Generate a simple, valid SQL query for DuckDB that answers this question:
                    "{query_str}"

                    DATABASE SCHEMA:
                    {schema_description}

                    Use only basic SQL constructs. Ensure the query is complete and valid.
                    ENSURE it has balanced parentheses and ends with a semicolon.
                    """
                else:
                    # Last attempt failed, return a fallback query
                    return "SELECT 'Failed to generate a valid SQL query' AS error;", query_context

        # Shouldn't reach here, but just in case
        return "SELECT 'Failed to generate a valid SQL query after multiple attempts' AS error;", query_context

    def _preprocess_query(self, query_string: str) -> tuple:
        """
        Preprocess the query string and extract context.
        Returns (cleaned_query, context_dict)
        """
        original_query = query_string
        print(f"📝 Processing query: '{query_string}'")

        # Initialize context
        context = {
            'original_query': original_query,
            'location': None,
            'division_breakdown': False,
            'format': 'default'
        }

        # Handle formatting requests
        format_patterns = {
            r'\b(?:as|in|format|show)\s+(?:a\s+)?(?:table|csv|json|markdown)\b': 'format',
            # Add more formatting patterns here
        }

        for pattern, key in format_patterns.items():
            if re.search(pattern, query_string, re.IGNORECASE):
                format_match = re.search(r'\b(table|csv|json|markdown)\b', query_string, re.IGNORECASE)
                if format_match:
                    context[key] = format_match.group(1).lower()
                    # Remove the formatting request from the query
                    query_string = re.sub(pattern, '', query_string, flags=re.IGNORECASE)

        # Check for division identifiers
        division_pattern = r'\b(?:division|div)\.?\s+([a-zA-Z])\b'
        division_match = re.search(division_pattern, query_string.lower())
        if division_match:
            division_letter = division_match.group(1).upper()
            context['division'] = division_letter
            print(f"Identified division: {division_letter}")

        # Check for time periods
        time_periods = {
            r'\btoday\b': ('day', "date = CURRENT_DATE"),
            r'\byesterday\b': ('day', "date = CURRENT_DATE - INTERVAL '1 day'"),
            r'\bthis\s+week\b': ('week', "date >= DATE_TRUNC('week', CURRENT_DATE) AND date < DATE_TRUNC('week', CURRENT_DATE) + INTERVAL '1 week'"),
            r'\blast\s+week\b': ('week', "date >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 week' AND date < DATE_TRUNC('week', CURRENT_DATE)"),
            r'\bthis\s+month\b': ('month', "date >= DATE_TRUNC('month', CURRENT_DATE) AND date < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'"),
            r'\blast\s+month\b': ('month', "date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' AND date < DATE_TRUNC('month', CURRENT_DATE)"),
            r'\bthis\s+year\b': ('year', "date >= DATE_TRUNC('year', CURRENT_DATE) AND date < DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1 year'"),
            r'\blast\s+year\b': ('year', "date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '1 year' AND date < DATE_TRUNC('year', CURRENT_DATE)"),
        }

        for pattern, (period, filter_sql) in time_periods.items():
            if re.search(pattern, query_string, re.IGNORECASE):
                context['time_period'] = period
                context['time_filter'] = filter_sql
                break

        # If no specific time period matched, default to all time
        if 'time_period' not in context:
            context['time_period'] = 'all'
            context['time_filter'] = "1=1"  # Always true condition

        # Clean and normalize the query
        cleaned_query = query_string.strip()
        cleaned_query = re.sub(r'\s+', ' ', cleaned_query)  # Normalize whitespace

        # Print the cleaned query
        print(f"Cleaned query (formatting requests removed): '{cleaned_query}'")

        return cleaned_query, context

    def _get_available_divisions(self):
        """
        Use the LLM to generate SQL that finds all available divisions/leagues in the database.
        Returns a list of division identifiers.
        """
        print("📝 Using LLM to identify available divisions/leagues")

        # Since we're having trouble with LLM and complex processing, let's just use our reliable SQL
        sql = """
        WITH extracted_divisions AS (
            SELECT DISTINCT REGEXP_EXTRACT(home_team, '\\(([A-Za-z0-9])\\)', 1) as division
            FROM matches
            WHERE REGEXP_EXTRACT(home_team, '\\(([A-Za-z0-9])\\)', 1) IS NOT NULL
            UNION
            SELECT DISTINCT REGEXP_EXTRACT(away_team, '\\(([A-Za-z0-9])\\)', 1) as division
            FROM matches
            WHERE REGEXP_EXTRACT(away_team, '\\(([A-Za-z0-9])\\)', 1) IS NOT NULL
        )
        SELECT division FROM extracted_divisions
        WHERE division IS NOT NULL AND division <> ''
        ORDER BY division;
        """

        try:
            print(f"📝 Executing direct division SQL: {sql}")
            division_results = self.sql_database.run_sql(sql)

            # Process results - handle DuckDB's return format
            divisions = []

            # Debug raw results
            print(f"📝 Raw division results type: {type(division_results)}")
            print(f"📝 Raw division results: {division_results}")

            # Special handling for DuckDB's specific return format
            # Sometimes it returns a tuple (results, metadata)
            if isinstance(division_results, tuple) and len(division_results) == 2:
                result_data = division_results[0]
                print(f"📝 Extracting from tuple: {result_data}")
                division_results = result_data

            # Handle dictionary result with 'result' key
            if isinstance(division_results, dict) and 'result' in division_results:
                print(f"📝 Extracting from dict with result key: {division_results['result']}")
                division_results = division_results['result']

            # Handle string that might contain serialized results
            if isinstance(division_results, str):
                # Try to extract values from string representation of list/tuple
                matches = re.findall(r"'([^']+)'", division_results)
                if matches:
                    print(f"📝 Extracted from string: {matches}")
                    divisions.extend(matches)
                else:
                    # If no matches found, just add the entire string
                    divisions.append(division_results)
            elif isinstance(division_results, list):
                # Process each item in the list
                for row in division_results:
                    if isinstance(row, tuple) and len(row) > 0:
                        # Extract from tuple
                        value = row[0]
                        if value and str(value).strip():
                            divisions.append(str(value).strip())
                    elif isinstance(row, dict):
                        # Extract from dict
                        value = next(iter(row.values()), None)
                        if value and str(value).strip():
                            divisions.append(str(value).strip())
                    else:
                        # Directly use value
                        if row and str(row).strip():
                            divisions.append(str(row).strip())

            # If still empty, use the default list
            if not divisions:
                print("📝 No divisions found, using defaults")
                return ["A", "B", "C", "D", "E"]

            print(f"📝 Final processed divisions: {divisions}")
            return divisions

        except Exception as e:
            print(f"Error getting divisions: {str(e)}")
            return ["A", "B", "C", "D", "E"]  # Fallback to defaults

    def _check_for_ambiguities(self, query_str, query_context):
        """
        Check if the query has ambiguities that need clarification before proceeding.
        Returns a tuple of (ambiguity_type, clarification_message) if clarification is needed,
        or None if no clarification is needed.
        """
        # Check for division ambiguity (mentions division/league without specifying which one)
        division_ambiguity = re.search(r'\b(league|division|div)\b', query_str.lower()) and not query_context.get('division')
        team_in_division = re.search(r'\b(team|best|worst|top|winning|highest|leading|winning).+\b(league|division|div)\b', query_str.lower())

        if division_ambiguity and team_in_division and not query_context.get('division'):
            print("📝 Detected division ambiguity in query")

            # Get available divisions using LLM
            divisions = self._get_available_divisions()

            return ("division", f"When you ask about '{query_str}', which division/league are you referring to? We have divisions {', '.join(divisions)} in our database.")

        # Check for team ambiguity (mentions 'team' but no specific team identified)
        team_ambiguity = re.search(r'\b(team)\b', query_str.lower()) and not query_context.get('team')
        specific_team_request = re.search(r'\b(stats|statistics|record|performance).+\b(team|for)\b', query_str.lower())

        if team_ambiguity and specific_team_request and not query_context.get('team'):
            print("📝 Detected team name ambiguity in query")
            return ("team", f"Which specific team would you like information about? Please provide the team name.")

        # No ambiguity detected
        return None

    def _is_empty_result(self, results):
        """Check if the query results are empty."""
        if results is None:
            return True

        if isinstance(results, tuple) and len(results) > 0:
            # Check for empty result in DuckDB format
            if results[0] == '[]' or results[0] == '{}':
                return True

        if isinstance(results, list) and not results:
            return True

        return False

    def _suggest_clarification_for_empty_results(self, query_str, query_context, sql):
        """Generate a helpful clarification message when results are empty."""
        # If we have a division in the context, suggest it might be wrong
        if query_context.get('division'):
            division = query_context.get('division')
            return f"I didn't find any results for Division {division}. Are you sure you meant Division {division}? Could it be a different division like A, B, C, or D?"

        # If we have a team in the context, suggest it might be wrong
        if query_context.get('team'):
            team = query_context.get('team')
            return f"I didn't find any data for the team '{team}'. Could you check the spelling or try another team name?"

        # If it's a time-specific query that returned no results
        if query_context.get('time_period') != 'all':
            time_period = query_context.get('time_period', 'specified time period')
            return f"I didn't find any matches for the {time_period} you specified. Would you like me to check for a broader time range?"

        # Generic empty result message
        return "I couldn't find any data matching your query. Could you try rephrasing or being more specific?"


def setup_query_engine(engine, conversation_history="", always_infer=False):
    """Set up the LlamaIndex query engine with the DuckDB database."""
    # Initialize with Anthropic's Claude 3.7 Sonnet model
    llm = Anthropic(
        api_key=os.environ["ANTHROPIC_API_KEY"],
        model="claude-3-7-sonnet-latest",
        temperature=0.0,  # Use deterministic output for SQL queries
        max_tokens=4000  # Increase max tokens to allow for more complete SQL responses
    )

    # Create the SQLDatabase instance explicitly including the 'matches' table
    sql_database = DuckDBSQLDatabase(engine, include_tables=["matches"])

    # Create the base context string
    base_context = """
    You are querying a soccer matches table with columns: date, home_team, away_team, home_score, away_score.
    The response will be automatically formatted into a table and statistics summary.
    Just focus on generating the correct SQL query for the user's request.
    """

    # Add conversation history if available
    if conversation_history:
        base_context += f"\n\n{conversation_history}"

    # Create the query engine with table context for natural language querying
    query_engine = CustomNLSQLTableQueryEngine(
        sql_database=sql_database,
        tables=["matches"],
        llm=llm,
        context_str=base_context,
        verbose=True,  # Enable SQL query logging
        always_infer=always_infer  # Pass the always_infer flag
    )

    return query_engine, engine


def run(conversation_history="", always_infer=False):
    """Compose the system by setting up the database and query engine, and return the query engine for use elsewhere."""
    engine = setup_database()
    query_engine, engine = setup_query_engine(engine, conversation_history, always_infer)
    return query_engine, engine


def main():
    """CLI for querying soccer match data using LlamaIndex."""
    parser = argparse.ArgumentParser(description="Query soccer match data using natural language.")
    parser.add_argument("query", help="The natural language query")
    parser.add_argument("--session-id", help="Session ID for conversation continuity")
    parser.add_argument("--never-infer", action="store_true", help="Disable dynamic inference (uses predefined templates)")
    args = parser.parse_args()

    # Set up session
    session_id = args.session_id
    if not session_id:
        session_id = memory_manager.create_session()

    # Get conversation history
    conversation_history = memory_manager.format_context(session_id)

    # Initialize the query engine
    query_engine, engine = run(conversation_history, always_infer=not args.never_infer)

    # Process the query
    response = query_engine.query(args.query, memory=memory_manager)

    # Print response
    print()
    print(response)
    print()
    print(f"Session ID: {session_id}")
    print("Use --session-id argument to continue this conversation")

    # Add interaction to memory
    memory_manager.add_interaction(
        session_id,
        args.query,
        str(response),
        query_engine.memory_context
    )


if __name__ == "__main__":
    main()