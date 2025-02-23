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
    # Extract potential team name phrases (2-3 word combinations)
    words = query.lower().split()
    phrases = []
    for i in range(len(words)):
        if i < len(words) - 1:
            phrases.append(' '.join(words[i:i+2]))  # 2-word phrases
        if i < len(words) - 2:
            phrases.append(' '.join(words[i:i+3]))  # 3-word phrases

    best_match = None
    best_score = 0
    best_phrase = None

    for phrase in phrases:
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


def setup_query_engine(engine):
    """Set up the LlamaIndex query engine with the DuckDB database."""
    # Initialize the OpenAI LLM; ensure OPENAI_API_KEY is set in your environment
    llm = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

    # Create the SQLDatabase instance explicitly including the 'matches' table
    sql_database = DuckDBSQLDatabase(engine, include_tables=["matches"])

    # Create the query engine with table context for natural language querying
    query_engine = NLSQLTableQueryEngine(
        sql_database=sql_database,
        tables=["matches"],
        llm=llm,
        context_str=(
            "This table contains information about soccer matches. "
            "The 'date' column contains the match date and time. "
            "The 'home_team' and 'away_team' columns contain team names. "
            "The 'home_score' and 'away_score' columns contain the final scores. "
            "The 'league' column contains the competition name. "
            "The 'type' column indicates if it's a regular season game, playoff, etc. "
            "Only completed matches are included (status = 1.0). "
            "\n\nTo get a team's performance, use two queries:"
            "\n\n1. Get match details:"
            "SELECT "
            "  date::DATE as Date, "
            "  CASE WHEN home_team = :team THEN 'vs' ELSE '@' END as H_A, "
            "  CASE WHEN home_team = :team THEN away_team ELSE home_team END as Opponent, "
            "  CASE WHEN home_team = :team THEN home_score ELSE away_score END as Goals_For, "
            "  CASE WHEN home_team = :team THEN away_score ELSE home_score END as Goals_Against, "
            "  CASE "
            "    WHEN (home_team = :team AND home_score > away_score) OR "
            "         (away_team = :team AND away_score > home_score) THEN 'W' "
            "    WHEN home_score = away_score THEN 'D' "
            "    ELSE 'L' "
            "  END as Result "
            "FROM matches "
            "WHERE (home_team = :team OR away_team = :team) "
            "  AND date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '3 month') "
            "  AND date < DATE_TRUNC('month', CURRENT_DATE - INTERVAL '2 month') "
            "ORDER BY date DESC; "
            "\n\n2. Get performance summary:"
            "WITH team_stats AS ("
            "  SELECT "
            "    COUNT(*) as games_played, "
            "    SUM(CASE WHEN "
            "      (home_team = :team AND home_score > away_score) OR "
            "      (away_team = :team AND away_score > home_score) "
            "    THEN 1 ELSE 0 END) as wins, "
            "    SUM(CASE WHEN home_score = away_score THEN 1 ELSE 0 END) as draws, "
            "    SUM(CASE WHEN "
            "      (home_team = :team AND home_score < away_score) OR "
            "      (away_team = :team AND away_score < home_score) "
            "    THEN 1 ELSE 0 END) as losses, "
            "    SUM(CASE WHEN home_team = :team THEN home_score ELSE away_score END) as goals_for, "
            "    SUM(CASE WHEN home_team = :team THEN away_score ELSE home_score END) as goals_against "
            "  FROM matches "
            "  WHERE (home_team = :team OR away_team = :team) "
            "    AND date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '3 month') "
            "    AND date < DATE_TRUNC('month', CURRENT_DATE - INTERVAL '2 month') "
            ") "
            "SELECT *, "
            "  goals_for - goals_against as goal_difference, "
            "  CAST(wins as FLOAT) / NULLIF(games_played, 0) * 100 as win_percentage "
            "FROM team_stats; "
            "\n\nWhen showing results:"
            "\n1. Format matches in a clear table with columns: Date, H/A, Opponent, For, Against, Result"
            "\n2. After the matches table, show a summary with:"
            "\n   - Record: W-D-L (total games)"
            "\n   - Goals: XX For, XX Against (goal difference)"
            "\n   - Win percentage"
            "\n3. Add a brief text summary of the team's overall performance"
            "\n4. Use 'vs' for home games and '@' for away games"
            "\n5. Use 'W', 'D', 'L' for results"
        )
    )

    return query_engine, engine


def run():
    """Compose the system by setting up the database and query engine, and return the query engine for use elsewhere."""
    engine = setup_database()
    query_engine, engine = setup_query_engine(engine)
    return query_engine, engine


def main():
    """CLI for querying soccer match data using LlamaIndex."""
    parser = argparse.ArgumentParser(description="Query soccer match data using natural language.")
    parser.add_argument("query", help="The natural language query to run against the database")
    args = parser.parse_args()

    if "OPENAI_API_KEY" not in os.environ:
        raise ValueError("Please set the OPENAI_API_KEY environment variable")

    try:
        # Compose the system and get the query engine
        query_engine, engine = run()

        # Preprocess the query to handle team names
        processed_query = preprocess_query(args.query, engine)
        if processed_query != args.query:
            print(f"Processed query: {processed_query}")

        # Run the query and print the response
        response = query_engine.query(processed_query)
        print(f"\nResponse: {response}")
    except Exception as e:
        print(f"\nError processing query: {e}")
        exit(1)


if __name__ == "__main__":
    main()