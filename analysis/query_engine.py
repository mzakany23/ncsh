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
        'had the', 'most', 'least', 'highest', 'lowest'
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
        self.memory_context = None  # Initialize memory context attribute

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

        # Look for match listing patterns - this should be checked early
        match_listing_patterns = [
            "game by game", "match by match", "show me games", "show me matches",
            "list games", "list matches", "each game", "each match", "all games",
            "all matches", "match results", "game results", "match history",
            "played against", "results of each", "detailed match", "match details"
        ]
        is_match_listing_query = any(pattern in query_lower for pattern in match_listing_patterns)

        # Check for match listing query early
        if is_match_listing_query:
            # Get time period information
            time_period, time_filter = self._get_time_period(query_str)

            # Try to identify team first
            teams = get_all_teams(self.sql_database._engine)
            team_match = find_best_matching_team(query_str, teams)
            matched_team = team_match[1] if team_match else None

            # Look for specific month mentions
            month_names = ["january", "february", "march", "april", "may", "june",
                         "july", "august", "september", "october", "november", "december"]
            detected_month = None
            for month in month_names:
                if month in query_lower:
                    detected_month = month
                    break
            if "last month" in query_lower:
                detected_month = "last"
                time_period = "last_month"
                time_filter = "date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' AND date < DATE_TRUNC('month', CURRENT_DATE)"

            # Look for year mentions
            detected_year = None
            year_match = re.search(r'(20\d{2})', query_str)
            if year_match:
                detected_year = year_match.group(1)
            if "this year" in query_lower:
                detected_year = "current"
            if "last year" in query_lower:
                detected_year = "previous"

            # Create query context directly
            query_context = {
                "team": matched_team,
                "time_period": time_period,
                "time_filter": time_filter,
                "query_type": "match_listing",
                "metrics": [],
                "month_filter": detected_month,
                "year_filter": detected_year,
                "comparison_team": None,
                "ranking_metric": None,
                "limit": 20,  # Show more results for match listings
                "format": "table",
                "format_requested": detected_format,
                "format_explanation": format_explanation,
                "explanation": "List detailed match results"
            }

            print(f"Match Listing Query Context: {query_context}")
            return query_context

        # Look for highest scoring game patterns - this must be checked FIRST
        highest_scoring_patterns = [
            "highest scoring game", "highest scoring match", "most goals in a game",
            "most goals in a match", "game with most goals", "match with most goals",
            "highest combined score", "most combined goals", "team had the highest scoring game"
        ]
        is_highest_scoring_query = any(pattern in query_lower for pattern in highest_scoring_patterns)

        # Check for highest scoring game query BEFORE other query types
        if is_highest_scoring_query:
            # Get time period information first
            time_period, time_filter = self._get_time_period(query_str)

            # Look for specific month mentions
            month_names = ["january", "february", "march", "april", "may", "june",
                         "july", "august", "september", "october", "november", "december"]
            detected_month = None
            for month in month_names:
                if month in query_lower:
                    detected_month = month
                    break
            if "last month" in query_lower:
                detected_month = "last"
                time_period = "last_month"
                time_filter = "date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' AND date < DATE_TRUNC('month', CURRENT_DATE)"

            # Look for year mentions
            detected_year = None
            year_match = re.search(r'(20\d{2})', query_str)
            if year_match:
                detected_year = year_match.group(1)
            if "this year" in query_lower:
                detected_year = "current"
            if "last year" in query_lower:
                detected_year = "previous"

            # Create query context directly
            query_context = {
                "team": None,  # No specific team for highest scoring game
                "time_period": time_period,
                "time_filter": time_filter,
                "query_type": "highest_scoring_games",
                "metrics": ["total_goals"],
                "month_filter": detected_month,
                "year_filter": detected_year,
                "comparison_team": None,
                "ranking_metric": "total_goals",
                "limit": 5,
                "format": "summary",
                "format_requested": detected_format,
                "format_explanation": format_explanation,
                "explanation": "Find matches with the highest combined scores"
            }

            print(f"Highest Scoring Games Query Context: {query_context}")
            return query_context

        # Look for team ranking patterns
        team_ranking_patterns = [
            "which team", "what team", "top team", "teams with", "teams that",
            "teams who", "best team", "worst team", "highest ranked", "lowest ranked",
            "team with most", "team with highest", "team with the most", "team with the highest",
            "highest scoring", "most scoring", "most goals", "most wins", "most losses",
            "team score", "team had the", "team with the", "which teams", "what teams"
        ]
        is_team_ranking_query = any(pattern in query_lower for pattern in team_ranking_patterns)

        # Identify potential ranking metric
        ranking_words = {
            "matches": "matches_played", "games": "matches_played",
            "goals": "goals_scored", "win": "win_percentage", "record": "win_percentage",
            "scoring": "goals_scored", "score": "goals_scored", "wins": "wins",
            "losses": "losses", "played": "matches_played"
        }

        ranking_metric = None
        for word, metric in ranking_words.items():
            if word in query_lower and is_team_ranking_query:
                ranking_metric = metric
                break

        # Look for hardest opponent indicators
        hardest_opponent_indicators = [
            "hardest opponent", "toughest opponent", "most difficult",
            "struggle against", "struggled against", "hard time against",
            "lost to", "lose to", "losing to", "difficult match",
            "who beat", "who defeated", "worst record against"
        ]

        # Look for aggregate query indicators
        aggregate_indicators = [
            "most games", "most matches", "highest number of games",
            "highest number of matches", "busiest day", "maximum games",
            "maximum matches", "total games", "total matches", "all games",
            "all teams", "all matches"
        ]

        # Enhanced patterns for daily aggregation
        daily_aggregation_patterns = [
            "per day", "per date", "in a single day", "on one day", "each day", "by day",
            "which day", "what day", "day with", "days with", "busiest day",
            "day having", "day that had", "day where", "days where",
            "most games in a day", "most matches in a day", "day having most",
            "day with most", "which day had the most", "what day had the most"
        ]

        # Check if any hardest opponent indicators are in the query
        has_hardest_opponent_intent = any(indicator in query_lower for indicator in hardest_opponent_indicators)

        # Check if any aggregate indicators are in the query
        has_aggregate_intent = any(indicator in query_lower for indicator in aggregate_indicators)

        # Look for "per day", "per date", "in a single day", "on one day" patterns
        has_daily_aggregation = any(pattern in query_lower for pattern in daily_aggregation_patterns)

        # Process team-ranking queries directly if detected
        if is_team_ranking_query:
            # Get time period information first
            time_period, time_filter = self._get_time_period(query_str)

            # Look for specific month mentions
            month_names = ["january", "february", "march", "april", "may", "june",
                         "july", "august", "september", "october", "november", "december"]
            detected_month = None
            for month in month_names:
                if month in query_lower:
                    detected_month = month
                    break
            if "last month" in query_lower:
                detected_month = "last"

            # Look for year mentions
            detected_year = None
            year_match = re.search(r'(20\d{2})', query_str)
            if year_match:
                detected_year = year_match.group(1)
            if "this year" in query_lower:
                detected_year = "current"
            if "last year" in query_lower:
                detected_year = "previous"

            # Create query context directly
            query_context = {
                "team": None,  # No specific team for team ranking queries
                "time_period": time_period,
                "time_filter": time_filter,
                "query_type": "team_rankings",
                "metrics": ["matches_played", "goals_scored", "win_percentage"],
                "month_filter": detected_month,
                "year_filter": detected_year,
                "comparison_team": None,
                "ranking_metric": ranking_metric,
                "limit": 10,
                "format": "summary",
                "format_requested": detected_format,
                "format_explanation": format_explanation,
                "explanation": f"Rank teams by {ranking_metric}"
            }

            print(f"Team Ranking Query Context: {query_context}")
            return query_context

        # Check for highest scoring game query
        if is_highest_scoring_query:
            # Get time period information first
            time_period, time_filter = self._get_time_period(query_str)

            # Look for specific month mentions
            month_names = ["january", "february", "march", "april", "may", "june",
                         "july", "august", "september", "october", "november", "december"]
            detected_month = None
            for month in month_names:
                if month in query_lower:
                    detected_month = month
                    break
            if "last month" in query_lower:
                detected_month = "last"

            # Look for year mentions
            detected_year = None
            year_match = re.search(r'(20\d{2})', query_str)
            if year_match:
                detected_year = year_match.group(1)
            if "this year" in query_lower:
                detected_year = "current"
            if "last year" in query_lower:
                detected_year = "previous"

            # Create query context directly
            query_context = {
                "team": None,  # No specific team for highest scoring game
                "time_period": time_period,
                "time_filter": time_filter,
                "query_type": "highest_scoring_games",
                "metrics": ["total_goals"],
                "month_filter": detected_month,
                "year_filter": detected_year,
                "comparison_team": None,
                "ranking_metric": "total_goals",
                "limit": 5,
                "format": "summary",
                "format_requested": detected_format,
                "format_explanation": format_explanation,
                "explanation": "Find matches with the highest combined scores"
            }

            print(f"Highest Scoring Games Query Context: {query_context}")
            return query_context

        # For non-team-ranking queries, continue with regular processing

        # Check if we have a day-focused query directly
        if has_daily_aggregation:
            # For day-focused queries, create a daily_stats query context
            time_period, time_filter = self._get_time_period(query_str)

            # Look for specific month mentions
            month_names = ["january", "february", "march", "april", "may", "june",
                         "july", "august", "september", "october", "november", "december"]
            detected_month = None
            for month in month_names:
                if month in query_lower:
                    detected_month = month
                    break
            if "last month" in query_lower:
                detected_month = "last"

            # Look for year mentions
            detected_year = None
            year_match = re.search(r'(20\d{2})', query_str)
            if year_match:
                detected_year = year_match.group(1)
            if "this year" in query_lower:
                detected_year = "current"
            if "last year" in query_lower:
                detected_year = "previous"

            query_context = {
                "team": None,  # No specific team for daily stats queries
                "time_period": time_period,
                "time_filter": time_filter,
                "query_type": "daily_stats",
                "metrics": ["matches_count", "teams_count", "goals"],
                "month_filter": detected_month,
                "year_filter": detected_year,
                "comparison_team": None,
                "ranking_metric": "matches_count",
                "limit": 10,
                "format": "summary",
                "format_requested": detected_format,
                "format_explanation": format_explanation,
                "explanation": "Calculate statistics aggregated by day"
            }

            print(f"Daily Stats Query Context (direct detection): {query_context}")
            return query_context

        # First try to identify team from memory if query uses pronouns
        remembered_team = memory.get_last_team() if memory else None

        # Check if pronouns are used in the query
        pronouns = ["they", "their", "them", "it", "its"]
        has_pronouns = any(pronoun in query_lower.split() for pronoun in pronouns)

        # If pronouns are used and we have a remembered team, use it directly
        if has_pronouns and remembered_team:
            print(f"Detected pronouns referring to previously mentioned team: {remembered_team}")
            query_str = query_str.replace("they", remembered_team).replace("their", f"{remembered_team}'s").replace("them", remembered_team)
            query_str = query_str.replace("They", remembered_team).replace("Their", f"{remembered_team}'s").replace("Them", remembered_team)
            query_str = query_str.replace("it", remembered_team).replace("its", f"{remembered_team}'s")
            query_str = query_str.replace("It", remembered_team).replace("Its", f"{remembered_team}'s")
            print(f"Rewritten query: {query_str}")
            query_lower = query_str.lower()

        # Try fuzzy matching for team name
        teams = get_all_teams(self.sql_database._engine)
        team_match = find_best_matching_team(query_str, teams)
        matched_team = team_match[1] if team_match else None

        # If we didn't match a team but have pronouns and a remembered team, explicitly use the remembered team
        if not matched_team and has_pronouns and remembered_team:
            matched_team = remembered_team

        # Get time period using focused prompt
        time_period, time_filter = self._get_time_period(query_str)

        # Look for hardest opponent indicators
        hardest_opponent_indicators = [
            "hardest opponent", "toughest opponent", "most difficult",
            "struggle against", "struggled against", "hard time against",
            "lost to", "lose to", "losing to", "difficult match",
            "who beat", "who defeated", "worst record against"
        ]

        # Look for aggregate query indicators
        aggregate_indicators = [
            "most games", "most matches", "highest number of games",
            "highest number of matches", "busiest day", "maximum games",
            "maximum matches", "total games", "total matches", "all games",
            "all teams", "all matches"
        ]

        # Check if any hardest opponent indicators are in the query
        has_hardest_opponent_intent = any(indicator in query_lower for indicator in hardest_opponent_indicators)

        # Check if any aggregate indicators are in the query
        has_aggregate_intent = any(indicator in query_lower for indicator in aggregate_indicators)

        # Look for ranking metrics
        ranking_metrics = []
        if "most matches" in query_lower or "most games" in query_lower:
            ranking_metrics.append("matches_played")
        if "most goals" in query_lower or "highest scoring" in query_lower:
            ranking_metrics.append("goals_scored")
        if "best record" in query_lower or "highest win" in query_lower:
            ranking_metrics.append("win_percentage")
        if "most wins" in query_lower:
            ranking_metrics.append("wins")
        if "most losses" in query_lower:
            ranking_metrics.append("losses")
        if not ranking_metrics and is_team_ranking_query:
            # Default to matches_played if no specific metric mentioned
            ranking_metrics = ["matches_played"]

        # Detect specific month mentions
        month_names = ["january", "february", "march", "april", "may", "june",
                       "july", "august", "september", "october", "november", "december"]
        month_map = {name: i+1 for i, name in enumerate(month_names)}

        detected_month = None
        for month in month_names:
            if month in query_lower:
                detected_month = month
                break

        # Check for "last month" in the query
        if "last month" in query_lower:
            time_period = "last_month"
            time_filter = "date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' AND date < DATE_TRUNC('month', CURRENT_DATE)"
            detected_month = "last"

        # Extract year if present
        detected_year = None
        year_match = re.search(r'\b(20\d{2})\b', query_str)
        if year_match:
            detected_year = year_match.group(1)
        elif "this year" in query_lower:
            detected_year = "current"
        elif "last year" in query_lower:
            detected_year = "previous"

        # Special handling for team ranking queries - force override other query types
        if is_team_ranking_query:
            # Determine default query structure for team rankings
            inferred_query_context = {
                "team": None,
                "time_period": time_period,
                "time_filter": time_filter,
                "query_type": "team_rankings",
                "metrics": ["matches_played", "goals_scored", "win_percentage"],
                "month_filter": detected_month,
                "year_filter": detected_year,
                "comparison_team": None,
                "ranking_metric": ranking_metrics[0] if ranking_metrics else "matches_played",
                "limit": 10,
                "format": "summary",
                "format_requested": detected_format,
                "format_explanation": format_explanation,
                "explanation": f"Rank teams by {ranking_metrics[0] if ranking_metrics else 'matches_played'}"
            }

            # Print debug info
            print(f"\nTeam Ranking Query Detected!")
            print(f"Inferred Query Context: {inferred_query_context}")

            # Directly return the inferred context for team ranking queries
            # to avoid misclassification by the LLM
            return inferred_query_context

        prompt = f"""
        Given the following user query, convert it into a structured representation.
        If the query uses pronouns (they, their, etc.) and we have a remembered team, use that team.

        User Query: {query_str}
        Remembered Team: {remembered_team}
        Matched Team: {matched_team}
        Detected Time Period: {time_period}
        Time Filter: {time_filter}
        Detected Month: {detected_month}
        Detected Year: {detected_year}
        Has aggregate statistics intent: {has_aggregate_intent}
        Has daily aggregation intent: {has_daily_aggregation}
        Has team ranking intent: {is_team_ranking_query}
        Detected ranking metrics: {ranking_metrics}

        IMPORTANT QUERY CLASSIFICATION RULES:
        - If the query asks about which teams the team struggled against, lost to, or had a hard time against, classify as "hardest_opponent"
        - If the query mentions "hardest", "toughest", "most difficult", "struggle", "difficult", classify as "hardest_opponent"
        - If the query asks who beat or defeated the team, classify as "hardest_opponent"
        - If the query asks about "most games/matches" or aggregated statistics without mentioning a specific team, classify as "aggregate_stats"
        - If the query asks about games/matches "per day", "in a single day", etc., classify as "daily_stats"
        - If the query asks for "which team", "what team", "top team", etc., classify as "team_rankings"
        - Pre-analysis shows hardest_opponent intent: {has_hardest_opponent_intent}
        - Pre-analysis shows aggregate statistics intent: {has_aggregate_intent}
        - Pre-analysis shows daily aggregation intent: {has_daily_aggregation}
        - Pre-analysis shows team ranking intent: {is_team_ranking_query}

        Extract and structure the following information:
        1. Team name (if any)
        2. Query type (CRITICAL - analyze carefully):
           - "stats" for general team statistics
           - "matches" for listing matches
           - "teams" for listing teams
           - "comparison" for comparing teams
           - "opponent_analysis" for analyzing how a team performed against specific opponents
           - "hardest_opponent" for finding opponents that the team struggled against, lost to most often, or had the worst record against
           - "best_performance" for finding games where the team performed best
           - "aggregate_stats" for statistics aggregated across all teams (no specific team mentioned)
           - "daily_stats" for statistics aggregated by day (e.g., most matches in a single day)
           - "team_rankings" for ranking teams by a specific metric (e.g., most matches played, most goals scored)
        3. Specific metrics requested (if any)
        4. Month/year filter (if any)
        5. Output format preferences (if any)

        Return the structured representation in this format:
        {{
            "team": "team name or null",
            "time_period": "{time_period}",
            "time_filter": "{time_filter}",
            "query_type": "stats/matches/teams/comparison/opponent_analysis/hardest_opponent/best_performance/aggregate_stats/daily_stats/team_rankings",
            "metrics": ["list", "of", "metrics"],
            "month_filter": "name of month if specified (e.g. january) or null",
            "year_filter": "specific year if mentioned (e.g. 2024) or null",
            "comparison_team": "opposing team name if relevant for comparison",
            "ranking_metric": "primary metric for ranking teams (matches_played/goals_scored/win_percentage/etc)",
            "limit": 5,
            "format": "table/summary/both",
            "explanation": "Brief explanation of what the user is asking for"
        }}

        Example responses:
        1. Query: "how did Key West do this month"
        {{
            "team": "Key West FC",
            "time_period": "month",
            "time_filter": "date >= DATE_TRUNC('month', CURRENT_DATE) AND date < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'",
            "query_type": "stats",
            "metrics": ["all"],
            "month_filter": null,
            "year_filter": null,
            "comparison_team": null,
            "ranking_metric": null,
            "limit": 5,
            "format": "summary",
            "explanation": "Get Key West FC's performance statistics for the current month"
        }}

        2. Query: "what was key wests hardest opponent in january of this year"
        {{
            "team": "Key West FC",
            "time_period": "month",
            "time_filter": "EXTRACT(month FROM date) = 1 AND EXTRACT(year FROM date) = EXTRACT(year FROM CURRENT_DATE)",
            "query_type": "hardest_opponent",
            "metrics": ["losses", "goals_against", "margin"],
            "month_filter": "january",
            "year_filter": "2024",
            "comparison_team": null,
            "ranking_metric": null,
            "limit": 5,
            "format": "summary",
            "explanation": "Find the opponent that Key West FC struggled the most against in January of the current year"
        }}

        3. Query: "what were the most games played in a single day last month"
        {{
            "team": null,
            "time_period": "last_month",
            "time_filter": "date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' AND date < DATE_TRUNC('month', CURRENT_DATE)",
            "query_type": "daily_stats",
            "metrics": ["games_count"],
            "month_filter": "last",
            "year_filter": null,
            "comparison_team": null,
            "ranking_metric": null,
            "limit": 5,
            "format": "summary",
            "explanation": "Find the day last month with the highest number of games played across all teams"
        }}

        4. Query: "which team played the most matches this year"
        {{
            "team": null,
            "time_period": "year",
            "time_filter": "date >= DATE_TRUNC('year', CURRENT_DATE)",
            "query_type": "team_rankings",
            "metrics": ["matches_played", "wins", "losses"],
            "month_filter": null,
            "year_filter": "current",
            "comparison_team": null,
            "ranking_metric": "matches_played",
            "limit": 10,
            "format": "summary",
            "explanation": "Rank teams by the number of matches they played this year"
        }}

        5. Query: "show me their matches"
        {{
            "team": "<remembered_team>",
            "time_period": "none",
            "time_filter": "1=1",
            "query_type": "matches",
            "metrics": [],
            "month_filter": null,
            "year_filter": null,
            "comparison_team": null,
            "ranking_metric": null,
            "limit": 5,
            "format": "table",
            "explanation": "Show all matches for the remembered team"
        }}

        Response:"""

        # Get structured representation from LLM
        response = self.llm.complete(prompt)
        try:
            query_context = eval(response.text)  # Convert string representation to dict

            # Additional validation check for hardest_opponent queries
            if has_hardest_opponent_intent and query_context.get("query_type") != "hardest_opponent":
                print("Detected hardest_opponent intent but model classified differently, overriding...")
                query_context["query_type"] = "hardest_opponent"
                if not query_context.get("explanation") or "hardest" not in query_context.get("explanation", ""):
                    query_context["explanation"] = f"Find the opponent that {query_context.get('team', 'the team')} struggled the most against"

            # Additional validation for aggregate queries
            if has_aggregate_intent and not query_context.get("team") and query_context.get("query_type") == "stats":
                print("Detected aggregate_stats intent but model classified as stats, overriding...")
                query_context["query_type"] = "aggregate_stats"
                query_context["explanation"] = "Calculate aggregated statistics across all teams"

            # Additional validation for daily aggregation queries
            if has_daily_aggregation and ("daily" not in query_context.get("query_type", "")):
                print("Detected daily aggregation intent but model classified differently, overriding...")
                query_context["query_type"] = "daily_stats"
                query_context["team"] = None  # Clear any team since this is about days
                query_context["explanation"] = "Calculate statistics aggregated by day"
                query_context["metrics"] = ["matches_count", "teams_count", "goals"]
                query_context["ranking_metric"] = "matches_count"

            # Additional validation for team ranking queries
            if is_team_ranking_query and query_context.get("query_type") != "team_rankings":
                print("Detected team_rankings intent but model classified differently, overriding...")
                query_context["query_type"] = "team_rankings"
                query_context["ranking_metric"] = ranking_metrics[0] if ranking_metrics else "matches_played"
                query_context["explanation"] = f"Rank teams by {query_context['ranking_metric']}"
        except:
            # Fallback to basic structure if LLM response isn't valid Python dict
            query_context = {
                "team": None,
                "time_period": time_period,
                "time_filter": time_filter,
                "query_type": "stats" if not has_hardest_opponent_intent else "hardest_opponent",
                "metrics": [],
                "month_filter": detected_month,
                "year_filter": detected_year,
                "comparison_team": None,
                "ranking_metric": ranking_metrics[0] if ranking_metrics else None,
                "limit": 5,
                "format": "summary",
                "format_requested": detected_format,
                "format_explanation": format_explanation,
                "explanation": "Fallback query understanding"
            }

            # For fallback, handle aggregate queries when no team is present
            if not query_context["team"]:
                if is_team_ranking_query:
                    query_context["query_type"] = "team_rankings"
                elif has_aggregate_intent:
                    query_context["query_type"] = "aggregate_stats"
                elif has_daily_aggregation:
                    query_context["query_type"] = "daily_stats"

        # Ensure we have a valid team name
        if not query_context.get("team"):
            # First try the fuzzy matched team
            if matched_team:
                query_context["team"] = matched_team
            # Then try the remembered team if we're using pronouns
            elif remembered_team and any(word in query_str.lower() for word in ["they", "their", "them"]):
                query_context["team"] = remembered_team

        # Ensure we have valid time period and filter
        if not query_context.get("time_period") or not query_context.get("time_filter"):
            query_context["time_period"] = time_period
            query_context["time_filter"] = time_filter

        # Direct month override based on detection in pre-processing
        if detected_month and not query_context.get("month_filter"):
            query_context["month_filter"] = detected_month

        # Direct year override based on detection in pre-processing
        if detected_year and not query_context.get("year_filter"):
            query_context["year_filter"] = "2024" if detected_year == "current" else detected_year

        # Handle specific month filtering
        if query_context.get("month_filter") and query_context["month_filter"] != "null":
            month_name = query_context["month_filter"].lower()

            # Special case for "last month"
            if month_name == "last":
                query_context["time_filter"] = "date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' AND date < DATE_TRUNC('month', CURRENT_DATE)"
            elif month_name in month_map:
                month_num = month_map[month_name]
                year_filter = ""
                if query_context.get("year_filter") and query_context["year_filter"] != "null":
                    year = query_context["year_filter"]
                    if year == "current":
                        year_filter = "AND EXTRACT(year FROM date) = EXTRACT(year FROM CURRENT_DATE)"
                    elif year == "previous":
                        year_filter = "AND EXTRACT(year FROM date) = EXTRACT(year FROM CURRENT_DATE) - 1"
                    else:
                        year_filter = f"AND EXTRACT(year FROM date) = {year}"
                else:
                    year_filter = "AND EXTRACT(year FROM date) = EXTRACT(year FROM CURRENT_DATE)"

                query_context["time_filter"] = f"EXTRACT(month FROM date) = {month_num} {year_filter}"

        # Set a default ranking metric if needed
        if query_context.get("query_type") == "team_rankings" and not query_context.get("ranking_metric"):
            query_context["ranking_metric"] = ranking_metrics[0] if ranking_metrics else "matches_played"

        # Add debugging output
        print(f"\nProcessed Query Context: {query_context}")
        return query_context

    def _generate_sql(self, query_context: dict) -> str:
        """Generate SQL query based on structured representation."""
        try:
            query_type = query_context.get('query_type', 'stats')
            team_name = query_context.get('team')
            time_filter = query_context.get('time_filter', '1=1')
            limit = query_context.get('limit', 5)

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
                        ROUND(100.0 * COUNT(CASE WHEN result = 'W' THEN 1 END) / NULLIF(COUNT(CASE WHEN result IN ('W', 'D', 'L') THEN 1 END), 0), 1) as win_percentage
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
                              NULLIF(COUNT(CASE WHEN result IN ('W', 'D', 'L') THEN 1 END), 0), 1) as loss_percentage
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

    def _format_response(self, results: list, query_context: dict) -> str:
        """Format the query results based on context."""
        if not results:
            return "No matches found for the specified time period."

        if isinstance(results[0], dict) and 'error' in results[0]:
            return str(results[0]['error'])

        # Format time period for display
        time_period = query_context.get('time_period', 'all')
        time_display = {
            'month': 'this month',
            'year': 'this year',
            'last_month': 'last month',
            'all': 'overall'
        }.get(time_period, time_period)

        # Handle month filter display
        if query_context.get('month_filter'):
            month_filter = query_context['month_filter']
            if month_filter != "null" and month_filter:
                if month_filter == "last":
                    time_display = "last month"
                else:
                    time_display = f"in {month_filter}"
                    if query_context.get('year_filter') and query_context['year_filter'] != "null":
                        time_display += f" {query_context['year_filter']}"

        # Format team name
        team_name = query_context.get('team', None)

        # Handle different query types with appropriate formatting
        query_type = query_context.get('query_type', 'stats')

        base_response = ""

        # For match listing
        if query_type == 'match_listing':
            if not results or len(results) == 0:
                return f"No matches found for {team_name or 'any team'} {time_display}."

            # Format match listing results
            match_results = []

            for match in results:
                date = match.get('date', 'Unknown date')
                home_team = match.get('home_team', 'Unknown')
                away_team = match.get('away_team', 'Unknown')
                home_score = match.get('home_score', 0)
                away_score = match.get('away_score', 0)
                venue = match.get('venue', '')
                result = match.get('result', '')

                # Handle not played games
                score_display = f"{home_score}-{away_score}" if home_score is not None and away_score is not None else "N/A"

                # Format the match differently based on whether a specific team was queried
                if team_name:
                    # For specific team queries, highlight their results
                    opponent = away_team if home_team.startswith(team_name) else home_team

                    if result == 'N/P':
                        match_results.append(
                            f"- {date}: vs {opponent} ({venue})\n"
                            f"  Status: Not played (scheduled)"
                        )
                    else:
                        scored = home_score if venue == 'home' else away_score
                        conceded = away_score if venue == 'home' else home_score
                        match_results.append(
                            f"- {date}: vs {opponent} ({venue})\n"
                            f"  Score: {scored}-{conceded} ({result})"
                        )
                else:
                    # For general match listing
                    if home_score is None or away_score is None:
                        match_results.append(
                            f"- {date}: {home_team} vs {away_team}\n"
                            f"  Status: Not played (scheduled)"
                        )
                    else:
                        match_results.append(
                            f"- {date}: {home_team} vs {away_team}\n"
                            f"  Score: {home_score}-{away_score}"
                        )

            # Fix the f-string syntax
            title = f"{team_name}'s matches {time_display}:" if team_name else f"All matches {time_display}:"
            base_response = f"{title}\n\n" + "\n\n".join(match_results)

        # For highest scoring games
        elif query_type == 'highest_scoring_games':
            if not results or len(results) == 0:
                return f"No match data found for the specified time period ({time_display})."

            # Format highest scoring games
            match_results = []

            for match_data in results:
                date = match_data.get('date', 'Unknown date')
                home_team = match_data.get('home_team', 'Unknown team')
                away_team = match_data.get('away_team', 'Unknown team')
                home_score = match_data.get('home_score', 0)
                away_score = match_data.get('away_score', 0)
                total_goals = match_data.get('total_goals', 0)

                # Only include games that were actually played
                if home_score is not None and away_score is not None:
                    match_results.append(
                        f"- {date}: {home_team} vs {away_team}\n"
                        f"  Score: {home_score}-{away_score} (total: {total_goals} goals)"
                    )

            if match_results:
                base_response = f"Highest scoring games {time_display}:\n\n" + "\n\n".join(match_results)
            else:
                return f"No match data found {time_display}."

        # For team rankings
        elif query_type == 'team_rankings':
            if not results or len(results) == 0:
                return f"No team ranking data found for the specified time period ({time_display})."

            # Get the ranking metric for display
            ranking_metric = query_context.get('ranking_metric', 'matches_played')
            ranking_metric_display = {
                'matches_played': 'matches played',
                'goals_scored': 'goals scored',
                'wins': 'wins',
                'win_percentage': 'win percentage'
            }.get(ranking_metric, ranking_metric)

            # Format team rankings
            team_results = []

            for i, team_stats in enumerate(results):
                team = team_stats.get('team_name', 'Unknown Team')
                matches = team_stats.get('matches_played', 0)
                wins = team_stats.get('wins', 0)
                draws = team_stats.get('draws', 0)
                losses = team_stats.get('losses', 0)
                not_played = team_stats.get('not_played', 0)
                goals_scored = team_stats.get('goals_scored', 0)
                win_pct = team_stats.get('win_percentage', 0)

                if ranking_metric == 'matches_played':
                    metric_value = matches
                elif ranking_metric == 'goals_scored':
                    metric_value = goals_scored
                elif ranking_metric == 'wins':
                    metric_value = wins
                elif ranking_metric == 'win_percentage':
                    metric_value = win_pct
                else:
                    metric_value = matches

                # Calculate played matches (excluding scheduled/not played)
                played_matches = matches - not_played

                team_results.append(
                    f"{i+1}. {team}: {metric_value} {ranking_metric_display}\n"
                    f"   Record: {wins}W-{draws}D-{losses}L in {played_matches} played matches ({not_played} scheduled/not played)\n"
                    f"   Goals: {goals_scored} scored, Win rate: {win_pct}%"
                )

            if team_results:
                base_response = f"Teams ranked by {ranking_metric_display} {time_display}:\n\n" + "\n\n".join(team_results)
            else:
                return f"No team ranking data found {time_display}."

        # For daily statistics
        elif query_type == 'daily_stats':
            if not results or len(results) == 0:
                return f"No daily match data found for the specified time period ({time_display})."

            # Format day-by-day match counts
            day_results = []

            for day_data in results:
                date = day_data.get('date', 'Unknown date')
                matches = day_data.get('matches_count', 0)
                teams = day_data.get('teams_count', 0)
                goals = day_data.get('total_goals', 0)
                avg_goals = day_data.get('avg_goals_per_match', 0)

                day_results.append(
                    f"- {date}: {matches} matches played with {teams} different teams\n"
                    f"  Total goals: {goals} (avg {avg_goals} per match)"
                )

            if day_results:
                base_response = f"Days with most matches {time_display}:\n\n" + "\n\n".join(day_results)
            else:
                return f"No match data found {time_display}."

        # For aggregate statistics
        elif query_type == 'aggregate_stats':
            if not results or len(results) == 0:
                return f"No aggregate statistics found for the specified time period ({time_display})."

            # Format aggregate statistics
            stats = results[0]
            total_matches = stats.get('total_matches', 0)
            days_with_matches = stats.get('days_with_matches', 0)
            avg_goals = stats.get('avg_goals_per_match', 0)
            total_goals = stats.get('total_goals', 0)
            highest_scoring = stats.get('highest_scoring_match', 0)
            teams_played = stats.get('teams_played', 0)

            base_response = f"Aggregate statistics {time_display}:\n\n"
            base_response += f"- Total matches played: {total_matches}\n"
            base_response += f"- Days with matches: {days_with_matches}\n"
            base_response += f"- Teams that played: {teams_played}\n"
            base_response += f"- Total goals scored: {total_goals}\n"
            base_response += f"- Average goals per match: {avg_goals}\n"
            base_response += f"- Highest scoring match: {highest_scoring} goals"

        # For hardest opponent analysis
        elif query_type == 'hardest_opponent':
            if not results or len(results) == 0:
                return f"No opponent data found for {team_name} {time_display}."

            # Format hardest opponent listing
            opponent_results = []

            for opponent in results:
                if 'hardest_opponent' in opponent:
                    opp_name = opponent['hardest_opponent']
                    games = opponent.get('games_played', 0)
                    wins = opponent.get('wins', 0)
                    draws = opponent.get('draws', 0)
                    losses = opponent.get('losses', 0)
                    not_played = opponent.get('not_played', 0)
                    goals_scored = opponent.get('goals_scored', 0)
                    goals_conceded = opponent.get('goals_conceded', 0)
                    loss_pct = opponent.get('loss_percentage', 0)

                    # Calculate played matches
                    played_matches = games - not_played

                    opponent_results.append(
                        f"- {opp_name}: {team_name} played {played_matches} matches with a record of {wins}W-{draws}D-{losses}L\n"
                        f"  ({not_played} scheduled matches not played)\n"
                        f"  Goals: {goals_scored} scored, {goals_conceded} conceded\n"
                        f"  Loss percentage: {loss_pct}%"
                    )

            if opponent_results:
                base_response = f"{team_name}'s hardest opponents {time_display}:\n\n" + "\n\n".join(opponent_results)
            else:
                return f"No clear 'hardest opponent' found for {team_name} {time_display}."

        # For match listing
        elif query_type == 'matches':
            # Format match listing
            matches = []
            for match in results:
                result = match.get('result', '')
                if result == 'N/P':
                    matches.append(f"- {match['date']}: {match['matchup']} (Not played)")
                else:
                    matches.append(f"- {match['date']}: {match['matchup']} ({match['goals_for']}-{match['goals_against']}, {result})")

            base_response = f"{team_name}'s matches {time_display}:\n" + "\n".join(matches)

        # For best performance
        elif query_type == 'best_performance':
            if not results or len(results) == 0:
                return f"No performance data found for {team_name} {time_display}."

            # Format best performances
            performances = []

            for match in results:
                if 'date' in match and 'opponent' in match:
                    date = match['date']
                    opponent = match['opponent']
                    goals_for = match.get('goals_for', 0)
                    goals_against = match.get('goals_against', 0)
                    margin = match.get('margin', 0)

                    performances.append(
                        f"- {date}: vs {opponent}, won {goals_for}-{goals_against} (margin: +{margin})"
                    )

            if performances:
                base_response = f"{team_name}'s best performances {time_display}:\n" + "\n".join(performances)
            else:
                return f"No standout performances found for {team_name} {time_display}."

        # For opponent analysis
        elif query_type == 'opponent_analysis':
            if not results or len(results) == 0:
                return f"No matches found between {team_name} and {query_context.get('comparison_team', 'the specified opponent')} {time_display}."

            # Format matches against specific opponent
            matches = []

            for match in results:
                date = match.get('date', 'Unknown date')
                matchup = match.get('matchup', 'Unknown matchup')
                team_score = match.get('team_score', 0)
                opponent_score = match.get('opponent_score', 0)
                result = match.get('result', '')

                matches.append(
                    f"- {date}: {matchup} ({team_score}-{opponent_score}, {result})"
                )

            comparison_team = query_context.get('comparison_team', 'specified opponent')
            base_response = f"{team_name}'s matches against {comparison_team} {time_display}:\n" + "\n".join(matches)

        # Default team statistics formatting
        else:
            # Format team statistics
            stats = results[0]  # Assuming one row of aggregate stats

            # Calculate additional metrics
            total_matches = stats.get('total_matches', 0) or stats.get('games_played', 0)
            wins = stats.get('wins', 0)
            draws = stats.get('draws', 0)
            losses = stats.get('losses', 0)
            not_played = stats.get('not_played', 0)
            goals_scored = stats.get('total_goals_scored', 0) or stats.get('goals_scored', 0)
            goals_conceded = stats.get('total_goals_conceded', 0) or stats.get('goals_conceded', 0)
            win_percentage = stats.get('win_percentage', 0)

            if total_matches == 0:
                return f"{team_name} has not played any matches {time_display}."

            # Calculate played matches
            played_matches = total_matches - not_played

            base_response = f"{team_name}'s performance {time_display}:\n"
            base_response += f"- Matches scheduled: {total_matches} total ({played_matches} played, {not_played} not played)\n"
            base_response += f"- Record: {wins}W {draws}D {losses}L\n"
            base_response += f"- Goals: {goals_scored} scored, {goals_conceded} conceded\n"
            base_response += f"- Win percentage: {win_percentage}%"

        # Check for format_requested and apply special formatting if needed
        if query_context.get('format_requested') and query_context.get('format_requested') != 'default':
            return self._apply_custom_formatting(base_response, results, query_context)

        return base_response

    def _apply_custom_formatting(self, base_response: str, results: list, query_context: dict) -> str:
        """Use Claude to apply custom formatting to the response."""
        format_type = query_context.get('format_requested', 'default')
        query_type = query_context.get('query_type', 'stats')

        # Provide specific instructions based on query type and format type
        format_instructions = ""
        if format_type == "table":
            if query_type == "match_listing":
                format_instructions = """
                For match listing data, create a well-formatted table with these columns:
                - Date
                - Opponent
                - Venue (Home/Away)
                - Score
                - Result (W/L/D)

                Make sure the table is neatly aligned with proper headers and separators.
                """
            elif query_type == "highest_scoring_games":
                format_instructions = """
                For highest scoring games, create a table with:
                - Date
                - Home Team
                - Away Team
                - Score (Home-Away)
                - Total Goals
                """
            elif query_type == "team_rankings":
                format_instructions = """
                For team rankings, create a table with:
                - Rank
                - Team Name
                - Matches Played
                - Record (W-D-L)
                - Goals Scored
                - Win %
                """
            elif query_type == "daily_stats":
                format_instructions = """
                For daily statistics, create a table with:
                - Date
                - Matches Count
                - Teams Involved
                - Total Goals
                - Avg Goals/Match
                """
            else:
                format_instructions = """
                Create a neatly formatted table from the data, with appropriate columns
                based on the data provided. Use clear headers and ensure alignment.
                """
        elif format_type == "markdown":
            format_instructions = """
            Format the response using proper markdown:
            - Use ## for the main title
            - Use ### for subtitles if needed
            - Use bullet points or numbered lists as appropriate
            - For tabular data, create a proper markdown table with | and - characters
            - Use **bold** for important information
            """

        # Prepare the formatting prompt for Claude
        prompt = f"""
        The following is a response about soccer match data:

        {base_response}

        Please reformat this response in a {format_type} format. The data is about {query_type} for soccer matches.

        {format_instructions}

        General formatting guidelines:
        - If format is "table", create a neatly formatted ASCII or markdown table.
        - If format is "chart", describe how the data would look in a chart format.
        - If format is "summary", create a concise summary of just the key points.
        - If format is "detailed", add more detailed explanations and analysis.
        - If format is "markdown", format using proper markdown with headers, lists, etc.

        Make the output clean, professional and highly readable while preserving all the important information.
        """

        # Use Claude to format the response
        response = self.llm.complete(prompt)
        formatted_response = response.text.strip()

        # If the response looks empty or invalid, fall back to the base response
        if not formatted_response or len(formatted_response) < 10:
            return base_response

        return formatted_response

    def query(self, query_str: str, **kwargs):
        """Execute the query pipeline: understand -> generate SQL -> format response."""
        memory = kwargs.get('memory', None)

        # Step 1: Understand the query
        query_context = self._understand_query(query_str, memory)

        # Print the identified query type and team
        print(f"\nIdentified query type: {query_context.get('query_type')}")
        if query_context.get("team"):
            print(f"Identified team: {query_context['team']}")

        # Store team in memory if found, along with query type
        if memory:
            if query_context.get("team"):
                memory.set_last_team(query_context["team"])

            # Store context for this query to be used when storing the interaction
            self.memory_context = {
                "matched_team": query_context.get("team"),
                "query_type": query_context.get("query_type", "stats")
            }

        # Step 2: Generate and execute SQL
        sql = self._generate_sql(query_context)
        print("\nGenerated SQL:")
        print(sql)

        # Execute the query
        with self.sql_database._engine.connect() as conn:
            result = conn.execute(text(sql))
            columns = result.keys()
            rows = [dict(zip(columns, row)) for row in result.fetchall()]

        # Step 3: Format the response
        formatted_response = self._format_response(rows, query_context)

        # Print the identified format if it was requested
        if query_context.get('format_requested') and query_context.get('format_requested') != 'default':
            print(f"\nDetected format request: {query_context.get('format_requested')}")

        return formatted_response


def setup_query_engine(engine, conversation_history=""):
    """Set up the LlamaIndex query engine with the DuckDB database."""
    # Initialize with Anthropic's Claude 3.7 Sonnet model
    llm = Anthropic(
        api_key=os.environ["ANTHROPIC_API_KEY"],
        model="claude-3-7-sonnet-latest",
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

    if "ANTHROPIC_API_KEY" not in os.environ:
        raise ValueError("Please set the ANTHROPIC_API_KEY environment variable")

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

        # Run the query and get the response - pass memory to the query engine
        response = query_engine.query(processed_query, memory=memory_manager)

        # The team is now stored in memory by the query engine itself
        # Get memory context if it was stored during query execution
        memory_context = getattr(query_engine, 'memory_context', None)

        # So we only need to store the interaction
        memory_manager.add_interaction(
            session_id=session_id,
            query=processed_query,
            response=str(response),
            context=memory_context
        )

        print(f"\nResponse: {response}")

        # Print session ID for continuing the conversation
        print(f"\nSession ID: {session_id}")
        print("Use --session-id argument to continue this conversation")
    except Exception as e:
        print(f"Error: {str(e)}")
        if os.getenv("DEBUG"):
            raise


if __name__ == "__main__":
    main()