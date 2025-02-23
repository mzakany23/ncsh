import os
from pathlib import Path
import boto3
from sqlalchemy import create_engine, text
from llama_index.core import SQLDatabase
from llama_index.core.indices.struct_store import NLSQLTableQueryEngine
from llama_index.llms.openai import OpenAI

def download_db_if_not_exists():
    """Download the DuckDB database from S3 if it doesn't exist locally."""
    db_path = Path("matches.parquet")
    if not db_path.exists():
        print("Downloading database from S3...")
        session = boto3.Session(profile_name='mzakany')
        s3 = session.client('s3', region_name='us-east-2')
        s3.download_file(
            'ncsh-app-data',
            'data/parquet/data.parquet',
            str(db_path)
        )
    return db_path

def setup_database():
    """Set up the DuckDB database connection."""
    # Ensure we have the database file
    db_path = download_db_if_not_exists()

    # Create DuckDB connection to read the parquet file
    engine = create_engine("duckdb:///:memory:", future=True)

    # Create and register the table
    with engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS matches AS SELECT * FROM read_parquet('{db_path}')"))
        # Verify table exists and has data
        result = conn.execute(text("SELECT COUNT(*) FROM matches")).scalar()
        print(f"Loaded {result} matches into database")

    return engine

def setup_query_engine(engine):
    """Set up the LlamaIndex query engine with the database."""
    # Initialize the OpenAI LLM
    llm = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

    # Create SQLDatabase instance with explicit dialect
    sql_database = SQLDatabase(engine, include_tables=["matches"])

    # Create the query engine with table context
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
            "Only completed matches are included (status = 1.0)."
        )
    )

    return query_engine

def main():
    # Check for OpenAI API key
    if "OPENAI_API_KEY" not in os.environ:
        raise ValueError("Please set the OPENAI_API_KEY environment variable")

    # Set up the database and query engine
    engine = setup_database()
    query_engine = setup_query_engine(engine)

    # Example queries
    queries = [
        "How many matches were played in total?",
        "Which team scored the most goals at home?",
        "What is the average number of goals scored per match?",
        "List the top 3 highest scoring matches"
    ]

    # Run queries
    print("\nRunning example queries:")
    print("-" * 50)
    for query in queries:
        print(f"\nQuery: {query}")
        response = query_engine.query(query)
        print(f"Response: {response}")

if __name__ == "__main__":
    main()