import os
from pathlib import Path
import boto3
import argparse
from sqlalchemy import create_engine, text
from llama_index.core import SQLDatabase
from llama_index.core.indices.struct_store import NLSQLTableQueryEngine
from llama_index.llms.openai import OpenAI
from thefuzz import fuzz
import re
import memory
from llama_index.core.response import Response

# Initialize conversation memory
memory_manager = memory.ConversationMemory()


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
        'give me', 'tell me', 'list the', 'what are', 'who are'
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
    teams = get_all_teams(engine)
    team_match = find_best_matching_team(query, teams)

    if team_match:
        original_phrase, matched_team = team_match
        print(f"Matched team name: '{original_phrase}' -> '{matched_team}'")
        # Replace the original phrase with the exact team name
        query = re.sub(re.escape(original_phrase), matched_team, query, flags=re.IGNORECASE)

    return query


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

    def __init__(self, sql_database, llm, **kwargs):
        """Initialize with SQL database and LLM."""
        super().__init__(sql_database=sql_database, **kwargs)
        self.sql_database = sql_database
        self.llm = llm

    def _get_table_context(self) -> str:
        """Override to provide table context."""
        return "Table 'matches' contains soccer match data with columns: date, home_team, away_team, home_score, away_score"

    def _get_team_stats(self, team_name=None):
        """Get comprehensive team statistics."""
        sql = """
        WITH team_matches AS (
          SELECT
            home_team as team,
            date,
            home_score as goals_for,
            away_score as goals_against,
            CASE
              WHEN home_score > away_score THEN 'W'
              WHEN home_score = away_score THEN 'D'
              ELSE 'L'
            END as result
          FROM matches
          WHERE EXTRACT(year FROM date) = EXTRACT(year FROM CURRENT_DATE)
          {team_filter_home}
          UNION ALL
          SELECT
            away_team as team,
            date,
            away_score as goals_for,
            home_score as goals_against,
            CASE
              WHEN away_score > home_score THEN 'W'
              WHEN away_score = home_score THEN 'D'
              ELSE 'L'
            END as result
          FROM matches
          WHERE EXTRACT(year FROM date) = EXTRACT(year FROM CURRENT_DATE)
          {team_filter_away}
        ),
        team_stats AS (
          SELECT
            team,
            COUNT(DISTINCT date) as games_played,
            COUNT(CASE WHEN result = 'W' THEN 1 END) as wins,
            COUNT(CASE WHEN result = 'D' THEN 1 END) as draws,
            COUNT(CASE WHEN result = 'L' THEN 1 END) as losses,
            SUM(goals_for) as goals_for,
            SUM(goals_against) as goals_against,
            SUM(goals_for - goals_against) as goal_diff,
            ROUND(CAST(COUNT(CASE WHEN result = 'W' THEN 1 END) as FLOAT) /
                  NULLIF(COUNT(DISTINCT date), 0) * 100, 1) as win_percentage,
            ROUND(CAST(COUNT(CASE WHEN result = 'L' THEN 1 END) as FLOAT) /
                  NULLIF(COUNT(DISTINCT date), 0) * 100, 1) as loss_percentage,
            ROUND(CAST(COUNT(CASE WHEN result = 'D' THEN 1 END) as FLOAT) /
                  NULLIF(COUNT(DISTINCT date), 0) * 100, 1) as draw_percentage,
            ROUND(CAST(SUM(goals_for) as FLOAT) /
                  NULLIF(COUNT(DISTINCT date), 0), 2) as goals_per_game,
            ROUND(CAST(SUM(goals_against) as FLOAT) /
                  NULLIF(COUNT(DISTINCT date), 0), 2) as goals_against_per_game
          FROM team_matches
          GROUP BY team
          HAVING COUNT(DISTINCT date) > 0
        )
        SELECT *
        FROM team_stats
        {order_and_limit}
        """

        # Add team filter if specified
        team_filter_home = f"AND home_team = '{team_name}'" if team_name else ""
        team_filter_away = f"AND away_team = '{team_name}'" if team_name else ""

        # Add ordering and limit
        order_and_limit = "ORDER BY games_played DESC, wins DESC LIMIT 10" if not team_name else ""

        # Format the SQL query
        sql = sql.format(
            team_filter_home=team_filter_home,
            team_filter_away=team_filter_away,
            order_and_limit=order_and_limit
        )

        print("\nGenerated SQL:")
        print(sql)

        # Execute the query
        with self.sql_database._engine.connect() as conn:
            result = conn.execute(text(sql))
            columns = result.keys()
            rows = [dict(zip(columns, row)) for row in result.fetchall()]

        return rows

    def _format_response_with_llm(self, data, query_str: str) -> str:
        """Use LLM to format the response based on the query and data."""
        if not data:
            return "No results found. Please check your query or try a different team name."

        # Create a prompt that includes the query and data
        prompt = f"""Given the following query and soccer team statistics, generate a natural response that directly answers the query.
Only include information that is relevant to the query. Format numbers nicely (e.g., percentages with one decimal place).

Query: {query_str}

Raw Statistics:
{data}

Instructions:
1. If the query asks for specific metrics (e.g., "how many games", "win percentage"), focus on those numbers
2. If the query asks for a general overview or comparison, include relevant statistics
3. If the query asks for a table, format it in markdown
4. Keep the response concise but informative
5. Include context where helpful (e.g., "this year" when talking about games played)
6. Round percentages to one decimal place
7. If showing a table, include a brief summary of key points below it

Response:"""

        # Get response from LLM
        response = self.llm.complete(prompt)
        return response.text

    def query(self, query_str: str, **kwargs):
        """Override to handle query execution and response formatting."""
        # First try to identify a team name
        teams = get_all_teams(self.sql_database._engine)
        team_match = find_best_matching_team(query_str, teams)

        # Get statistics (either for specific team or top teams)
        team_name = team_match[1] if team_match else None
        stats = self._get_team_stats(team_name)

        # Format response using LLM
        response = self._format_response_with_llm(stats, query_str)
        return response


def setup_query_engine(engine, conversation_history=""):
    """Set up the LlamaIndex query engine with the DuckDB database."""
    # Initialize the OpenAI LLM with GPT-4o
    llm = OpenAI(
        api_key=os.environ["OPENAI_API_KEY"],
        model="gpt-4o",
        temperature=0.0  # Use deterministic output for SQL queries
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
        verbose=True  # Enable SQL query logging
    )

    return query_engine, engine


def run(conversation_history=""):
    """Compose the system by setting up the database and query engine, and return the query engine for use elsewhere."""
    engine = setup_database()
    query_engine, engine = setup_query_engine(engine, conversation_history)
    return query_engine, engine


def main():
    """CLI for querying soccer match data using LlamaIndex."""
    parser = argparse.ArgumentParser(description="Query soccer match data using natural language.")
    parser.add_argument("query", help="The natural language query to run against the database")
    parser.add_argument("--session-id", help="Session ID for conversation continuity")
    args = parser.parse_args()

    if "OPENAI_API_KEY" not in os.environ:
        raise ValueError("Please set the OPENAI_API_KEY environment variable")

    try:
        # Get or create session ID
        session_id = args.session_id or memory_manager.create_session()
        if not args.session_id:
            print(f"Created new session: {session_id}")

        # Get conversation history
        conversation_history = memory_manager.format_context(session_id)

        # Compose the system and get the query engine
        query_engine, engine = run(conversation_history)

        # Preprocess the query to handle team names
        processed_query = preprocess_query(args.query, engine)
        if processed_query != args.query:
            print(f"Processed query: {processed_query}")

        # Run the query and get the response
        response = query_engine.query(processed_query)

        # Store the interaction in memory
        memory_manager.add_interaction(
            session_id=session_id,
            query=processed_query,
            response=str(response),
            context={"matched_team": processed_query if processed_query != args.query else None}
        )

        print(f"\nResponse: {response}")
        print(f"\nSession ID: {session_id}")
        print("Use --session-id argument to continue this conversation")

    except Exception as e:
        print(f"\nError processing query: {e}")
        exit(1)


if __name__ == "__main__":
    main()