"""Team and division utilities for the Query Engine."""

import re
from thefuzz import fuzz
from typing import List, Dict, Tuple, Optional, Any, Union

def get_all_teams(engine) -> List[str]:
    """
    Get all team names from the database.

    Args:
        engine: SQL database engine

    Returns:
        List of team names
    """
    team_query = """
    SELECT DISTINCT home_team FROM matches
    UNION
    SELECT DISTINCT away_team FROM matches;
    """

    try:
        teams_result = engine.run_sql(team_query)
        teams = []

        # Handle tuple format from DuckDB
        if isinstance(teams_result, tuple) and len(teams_result) > 0:
            # Process the result data
            if len(teams_result) > 1 and isinstance(teams_result[1], dict):
                result_data = teams_result[1].get('result', [])
                for row in result_data:
                    if row and row[0]:
                        teams.append(row[0])

        return teams
    except Exception as e:
        print(f"Error getting teams: {str(e)}")
        return []


def find_best_matching_team(query: str, teams: List[str], threshold: int = 80) -> Optional[Tuple[str, str]]:
    """
    Find the best matching team name in the query.

    Args:
        query: The query string
        teams: List of team names to match against
        threshold: Minimum score to consider a match

    Returns:
        Tuple of (matched phrase, team name) or None if no match found
    """
    # Skip phrases that are likely not team names
    skip_phrases = [
        'how many', 'how much', 'who is', 'who are', 'what is', 'what are',
        'when is', 'when are', 'where is', 'where are', 'which team', 'vs', 'versus',
        'wins', 'losses', 'draws', 'team', 'teams', 'club', 'clubs', 'division', 'league'
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


def get_teams_by_division(engine) -> Dict[str, Any]:
    """
    Get a mapping of teams grouped by their divisions to provide as context.
    This helps the LLM understand what leagues/divisions exist and what teams are in each.

    Args:
        engine: SQL database engine

    Returns:
        Dict containing teams by division and formatted context string
    """
    print("📝 Getting teams grouped by division for context")

    # Query to extract teams and their divisions
    sql = """
    WITH team_divisions AS (
        -- Extract teams and divisions from home teams
        SELECT DISTINCT
            REGEXP_REPLACE(home_team, '\\s*\\([^)]*\\)\\s*$', '') as team_name,
            REGEXP_EXTRACT(home_team, '\\(([A-Za-z0-9])\\)', 1) as division
        FROM matches
        WHERE REGEXP_EXTRACT(home_team, '\\(([A-Za-z0-9])\\)', 1) IS NOT NULL

        UNION

        -- Extract teams and divisions from away teams
        SELECT DISTINCT
            REGEXP_REPLACE(away_team, '\\s*\\([^)]*\\)\\s*$', '') as team_name,
            REGEXP_EXTRACT(away_team, '\\(([A-Za-z0-9])\\)', 1) as division
        FROM matches
        WHERE REGEXP_EXTRACT(away_team, '\\(([A-Za-z0-9])\\)', 1) IS NOT NULL
    )
    SELECT team_name, division FROM team_divisions
    ORDER BY division, team_name;
    """

    try:
        results = engine.run_sql(sql)

        # Process the results into a structured format
        teams_by_division = {}

        if isinstance(results, tuple) and len(results) > 0:
            result_data = results[1].get('result', [])

            for row in result_data:
                team_name, division = row

                if division not in teams_by_division:
                    teams_by_division[division] = []

                teams_by_division[division].append(team_name)

        # Log the result
        print(f"📝 Found {len(teams_by_division)} divisions with {sum(len(teams) for teams in teams_by_division.values())} teams")

        # Create a formatted string for context
        context = []
        for division, teams in teams_by_division.items():
            team_list = ", ".join(teams[:10])  # Limit to 10 teams per division to control context size
            team_count = len(teams)

            if team_count > 10:
                team_list += f", and {team_count - 10} more teams"

            context.append(f"Division {division}: {team_list}")

        return {
            "teams_by_division": teams_by_division,
            "division_context": "\n".join(context)
        }

    except Exception as e:
        print(f"Error getting teams by division: {str(e)}")
        return {
            "teams_by_division": {},
            "division_context": ""
        }


def get_available_divisions(engine) -> List[str]:
    """
    Get all available divisions from the database.

    Args:
        engine: SQL database engine

    Returns:
        List of division identifiers
    """
    print("📝 Identifying available divisions/leagues")

    # SQL to extract all divisions
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

    print(f"📝 Executing division SQL: {sql}")

    try:
        results = engine.run_sql(sql)
        print(f"📝 Raw division results type: {type(results)}")
        print(f"📝 Raw division results: {results}")

        # Extract the actual divisions from the results
        divisions = []
        if isinstance(results, tuple) and len(results) > 0:
            result_data = results[1].get('result', [])
            print(f"📝 Extracting from tuple: {result_data}")
            divisions = [row[0] for row in result_data]
        elif isinstance(results, str):
            # Try to extract from string representation
            import re
            div_matches = re.findall(r"\('([^']+)'\)", results)
            print(f"📝 Extracted from string: {div_matches}")
            divisions = div_matches

        print(f"📝 Final processed divisions: {divisions}")
        return divisions

    except Exception as e:
        print(f"Error getting divisions: {str(e)}")
        return []