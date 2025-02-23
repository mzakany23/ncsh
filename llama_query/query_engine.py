import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from llama_index.core import SQLDatabase
from llama_index.core.indices.struct_store import NLSQLTableQueryEngine
from llama_index.llms.openai import OpenAI

# Load environment variables
load_dotenv()

def setup_database():
    """Set up the DuckDB database with a sample table."""
    # Create an in-memory DuckDB instance
    engine = create_engine("duckdb:///:memory:")

    # Create table using raw SQL
    with engine.connect() as conn:
        # Create the table
        conn.execute(text("""
            CREATE TABLE players (
                id INTEGER PRIMARY KEY,
                name VARCHAR,
                team VARCHAR,
                position VARCHAR COLLATE NOCASE,
                goals INTEGER
            )
        """))

        # Insert sample data
        conn.execute(text("""
            INSERT INTO players (id, name, team, position, goals) VALUES
            (1, 'John Smith', 'Red Dragons', 'Forward', 12),
            (2, 'Sarah Johnson', 'Blue Eagles', 'Midfielder', 8),
            (3, 'Mike Wilson', 'Green Lions', 'Defender', 2),
            (4, 'Emma Brown', 'Red Dragons', 'Forward', 15)
        """))
        conn.commit()

    return engine

def setup_query_engine(engine):
    """Set up the LlamaIndex query engine with the database."""
    # Initialize the OpenAI LLM
    llm = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    # Create SQLDatabase instance
    sql_database = SQLDatabase(engine)

    # Create the query engine with table context
    query_engine = NLSQLTableQueryEngine(
        sql_database=sql_database,
        tables=["players"],
        llm=llm,
        context_str=(
            "This table contains information about soccer players. "
            "The 'position' column contains values like 'Forward', 'Midfielder', or 'Defender'. "
            "The 'goals' column represents the number of goals scored by each player. "
            "The 'team' column contains the team name for each player."
        )
    )

    return query_engine

def main():
    # Check for OpenAI API key
    if not os.getenv("OPENAI_API_KEY"):
        raise ValueError("Please set the OPENAI_API_KEY environment variable")

    # Set up the database and query engine
    engine = setup_database()
    query_engine = setup_query_engine(engine)

    # Example queries
    queries = [
        "How many players are on the Red Dragons team?",
        "Who has scored the most goals?",
        "List all forwards in the database",
        "What is the average number of goals scored by midfielders?"
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