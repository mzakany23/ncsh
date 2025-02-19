#!/usr/bin/env python3

import duckdb
import sys
import os
from pathlib import Path

def get_parquet_path():
    """Get the path to the Parquet file, ensuring the data directory exists"""
    # Get the project root directory (parent of scripts directory)
    project_root = Path(__file__).parent.parent
    parquet_path = project_root / 'data' / 'parquet' / 'data.parquet'

    # Ensure the data/parquet directory exists
    parquet_dir = parquet_path.parent
    parquet_dir.mkdir(parents=True, exist_ok=True)

    return str(parquet_path)

def main():
    # Get the path to the Parquet file
    parquet_path = get_parquet_path()

    # Check if the Parquet file exists
    if not os.path.exists(parquet_path):
        print(f"Error: Parquet file not found at {parquet_path}")
        print("Please ensure the data has been processed and exists in the correct location.")
        return

    print(f"Reading data from: {parquet_path}")

    try:
        # Connect to DuckDB (in-memory)
        con = duckdb.connect()

        # Register the Parquet file as a table
        con.execute(f"CREATE TABLE games AS SELECT * FROM read_parquet('{parquet_path}')")

        # Print schema
        print("\nSchema:")
        print(con.execute("DESCRIBE games").fetchall())

        # Print sample data
        print("\nSample data (5 rows):")
        print(con.execute("SELECT * FROM games LIMIT 5").fetchdf())

        # Print some basic statistics
        print("\nBasic statistics:")
        print("\nGames by league:")
        print(con.execute("""
            SELECT league, COUNT(*) as game_count
            FROM games
            GROUP BY league
            ORDER BY game_count DESC
        """).fetchdf())

        print("\nGames by month:")
        print(con.execute("""
            SELECT
                EXTRACT(YEAR FROM date) as year,
                EXTRACT(MONTH FROM date) as month,
                COUNT(*) as game_count
            FROM games
            GROUP BY year, month
            ORDER BY year, month
        """).fetchdf())

        # Keep connection open for interactive queries if run with --interactive flag
        if len(sys.argv) > 1 and sys.argv[1] == '--interactive':
            print("\nEntering interactive mode. Type 'exit' to quit.")
            print("Example queries:")
            print("1. SELECT COUNT(*) FROM games;")
            print("2. SELECT * FROM games WHERE home_score > 5 LIMIT 5;")
            print("3. SELECT league, AVG(home_score + away_score) as avg_total_score FROM games GROUP BY league ORDER BY avg_total_score DESC;")

            while True:
                try:
                    query = input("\nEnter SQL query (or 'exit' to quit): ")
                    if query.lower() == 'exit':
                        break
                    if query.strip():  # Only execute if query is not empty
                        result = con.execute(query).fetchdf()
                        print("\nResult:")
                        print(result)
                except Exception as e:
                    print(f"Error: {str(e)}")

    except Exception as e:
        print(f"Error querying data: {str(e)}")
    finally:
        con.close()

if __name__ == '__main__':
    main()