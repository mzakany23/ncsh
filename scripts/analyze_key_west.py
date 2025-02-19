#!/usr/bin/env python3

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
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

def analyze_key_west():
    """Analyze Key West team performance data"""
    # Get the path to the Parquet file
    parquet_path = get_parquet_path()

    # Check if the Parquet file exists
    if not os.path.exists(parquet_path):
        print(f"Error: Parquet file not found at {parquet_path}")
        print("Please ensure the data has been processed and exists in the correct location.")
        return

    print(f"Reading data from: {parquet_path}")

    try:
        # Connect to DuckDB
        con = duckdb.connect()

        # Register the Parquet file
        con.execute(f"CREATE TABLE games AS SELECT * FROM read_parquet('{parquet_path}')")

        # First, let's find all variations of the Key West team name
        print("\nKey West Team Name Variations:")
        variations = con.execute("""
            SELECT DISTINCT
                CASE
                    WHEN home_team LIKE '%Key West%' THEN home_team
                    ELSE away_team
                END as team_name
            FROM games
            WHERE home_team LIKE '%Key West%' OR away_team LIKE '%Key West%'
        """).fetchdf()
        print(variations)

        # Get all Key West games (both home and away)
        print("\nAll Key West Games:")
        key_west_games = con.execute("""
            SELECT
                date,
                CASE
                    WHEN home_team LIKE '%Key West%' THEN home_team
                    ELSE away_team
                END as key_west_team,
                CASE
                    WHEN home_team LIKE '%Key West%' THEN away_team
                    ELSE home_team
                END as opponent,
                CASE
                    WHEN home_team LIKE '%Key West%' THEN home_score
                    ELSE away_score
                END as key_west_score,
                CASE
                    WHEN home_team LIKE '%Key West%' THEN away_score
                    ELSE home_score
                END as opponent_score,
                league,
                time,
                CASE
                    WHEN home_team LIKE '%Key West%' THEN 'home'
                    ELSE 'away'
                END as venue
            FROM games
            WHERE home_team LIKE '%Key West%' OR away_team LIKE '%Key West%'
            ORDER BY date
        """).fetchdf()

        if len(key_west_games) == 0:
            print("\nNo games found for Key West team.")
            return

        print("\nTotal Key West games found:", len(key_west_games))
        print("\nSample of recent games:")
        print(key_west_games.tail())

        # League distribution
        print("\nLeagues Key West plays in:")
        league_dist = con.execute("""
            SELECT league, COUNT(*) as games_played
            FROM games
            WHERE home_team LIKE '%Key West%' OR away_team LIKE '%Key West%'
            GROUP BY league
            ORDER BY games_played DESC
        """).fetchdf()
        print(league_dist)

        # Win/Loss record overall
        print("\nOverall Win/Loss Record:")
        record = con.execute("""
            SELECT
                COUNT(*) as total_games,
                SUM(CASE WHEN key_west_score > opponent_score THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN key_west_score < opponent_score THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN key_west_score = opponent_score THEN 1 ELSE 0 END) as draws,
                ROUND(AVG(key_west_score), 2) as avg_goals_for,
                ROUND(AVG(opponent_score), 2) as avg_goals_against,
                ROUND(AVG(key_west_score - opponent_score), 2) as avg_goal_difference
            FROM (
                SELECT
                    CASE
                        WHEN home_team LIKE '%Key West%' THEN home_score
                        ELSE away_score
                    END as key_west_score,
                    CASE
                        WHEN home_team LIKE '%Key West%' THEN away_score
                        ELSE home_score
                    END as opponent_score
                FROM games
                WHERE home_team LIKE '%Key West%' OR away_team LIKE '%Key West%'
            )
        """).fetchdf()
        print(record)

        # Home vs Away record
        print("\nHome vs Away Record:")
        venue_record = con.execute("""
            SELECT
                venue,
                COUNT(*) as games,
                SUM(CASE WHEN key_west_score > opponent_score THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN key_west_score < opponent_score THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN key_west_score = opponent_score THEN 1 ELSE 0 END) as draws,
                ROUND(AVG(key_west_score), 2) as avg_goals_for,
                ROUND(AVG(opponent_score), 2) as avg_goals_against
            FROM (
                SELECT
                    CASE
                        WHEN home_team LIKE '%Key West%' THEN home_score
                        ELSE away_score
                    END as key_west_score,
                    CASE
                        WHEN home_team LIKE '%Key West%' THEN away_score
                        ELSE home_score
                    END as opponent_score,
                    CASE
                        WHEN home_team LIKE '%Key West%' THEN 'home'
                        ELSE 'away'
                    END as venue
                FROM games
                WHERE home_team LIKE '%Key West%' OR away_team LIKE '%Key West%'
            )
            GROUP BY venue
        """).fetchdf()
        print(venue_record)

        # Most common opponents with record against each
        print("\nRecord Against Most Common Opponents:")
        opponent_record = con.execute("""
            WITH opponent_games AS (
                SELECT
                    CASE
                        WHEN home_team LIKE '%Key West%' THEN away_team
                        ELSE home_team
                    END as opponent,
                    CASE
                        WHEN home_team LIKE '%Key West%' THEN home_score
                        ELSE away_score
                    END as key_west_score,
                    CASE
                        WHEN home_team LIKE '%Key West%' THEN away_score
                        ELSE home_score
                    END as opponent_score
                FROM games
                WHERE home_team LIKE '%Key West%' OR away_team LIKE '%Key West%'
            )
            SELECT
                opponent,
                COUNT(*) as games_played,
                SUM(CASE WHEN key_west_score > opponent_score THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN key_west_score < opponent_score THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN key_west_score = opponent_score THEN 1 ELSE 0 END) as draws,
                ROUND(AVG(key_west_score), 2) as avg_goals_for,
                ROUND(AVG(opponent_score), 2) as avg_goals_against
            FROM opponent_games
            GROUP BY opponent
            HAVING games_played >= 2
            ORDER BY games_played DESC, wins DESC
        """).fetchdf()
        print(opponent_record)

        # Biggest wins and losses
        print("\nBiggest Wins:")
        biggest_wins = con.execute("""
            SELECT
                date,
                key_west_team,
                opponent,
                key_west_score,
                opponent_score,
                key_west_score - opponent_score as goal_difference,
                league
            FROM (
                SELECT
                    date,
                    CASE
                        WHEN home_team LIKE '%Key West%' THEN home_team
                        ELSE away_team
                    END as key_west_team,
                    CASE
                        WHEN home_team LIKE '%Key West%' THEN away_team
                        ELSE home_team
                    END as opponent,
                    CASE
                        WHEN home_team LIKE '%Key West%' THEN home_score
                        ELSE away_score
                    END as key_west_score,
                    CASE
                        WHEN home_team LIKE '%Key West%' THEN away_score
                        ELSE home_score
                    END as opponent_score,
                    league
                FROM games
                WHERE home_team LIKE '%Key West%' OR away_team LIKE '%Key West%'
            )
            WHERE key_west_score > opponent_score
            ORDER BY goal_difference DESC
            LIMIT 5
        """).fetchdf()
        print(biggest_wins)

        print("\nBiggest Losses:")
        biggest_losses = con.execute("""
            SELECT
                date,
                key_west_team,
                opponent,
                key_west_score,
                opponent_score,
                opponent_score - key_west_score as goal_difference,
                league
            FROM (
                SELECT
                    date,
                    CASE
                        WHEN home_team LIKE '%Key West%' THEN home_team
                        ELSE away_team
                    END as key_west_team,
                    CASE
                        WHEN home_team LIKE '%Key West%' THEN away_team
                        ELSE home_team
                    END as opponent,
                    CASE
                        WHEN home_team LIKE '%Key West%' THEN home_score
                        ELSE away_score
                    END as key_west_score,
                    CASE
                        WHEN home_team LIKE '%Key West%' THEN away_score
                        ELSE home_score
                    END as opponent_score,
                    league
                FROM games
                WHERE home_team LIKE '%Key West%' OR away_team LIKE '%Key West%'
            )
            WHERE opponent_score > key_west_score
            ORDER BY goal_difference DESC
            LIMIT 5
        """).fetchdf()
        print(biggest_losses)

    except Exception as e:
        print(f"Error analyzing data: {str(e)}")
    finally:
        # Close connection
        con.close()

if __name__ == '__main__':
    analyze_key_west()