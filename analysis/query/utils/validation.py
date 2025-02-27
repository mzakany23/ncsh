"""Data validation utilities for the Query Engine."""

import re
from typing import Any

def is_empty_result(results: Any) -> bool:
    """
    Check if the query results are empty or invalid.

    Args:
        results: The results to check

    Returns:
        bool: True if the results are empty, False otherwise
    """
    if results is None:
        return True

    # Handle tuple format from DuckDB
    if isinstance(results, tuple) and len(results) > 0:
        # Check first element for string representation
        if isinstance(results[0], str) and not results[0].strip():
            return True

        # Check for empty result lists
        if len(results) > 1 and isinstance(results[1], dict):
            result_data = results[1].get('result', [])
            if not result_data or (len(result_data) == 1 and not any(result_data[0])):
                return True

    # Handle empty list/tuple results
    if isinstance(results, (list, tuple)) and len(results) == 0:
        return True

    # Handle empty dict results
    if isinstance(results, dict) and not results:
        return True

    # Convert to string and check if it contains any actual data
    result_str = str(results)
    if result_str in ["[]", "()", "{}", "None", "''", '""']:
        return True

    return False


def has_unrealistic_values(results: Any, max_goals: int = 100) -> bool:
    """
    Check if the results contain unrealistic values for soccer statistics.
    This helps identify potentially wrong or buggy SQL queries.

    Args:
        results: The results from a SQL query
        max_goals: Maximum reasonable number of goals

    Returns:
        bool: True if the results have unrealistic values, False otherwise
    """
    if is_empty_result(results):
        return False

    # Define patterns to capture goal-related columns with high values
    goal_patterns = [
        r'(\d+)\s+goals',  # JSON format
        r'goals.*?:\s*(\d+)',  # Key-value format
        r'goals_for.*?:\s*(\d+)',
        r'goals_against.*?:\s*(\d+)',
        r'goals_scored.*?:\s*(\d+)',
        r'goals_conceded.*?:\s*(\d+)'
    ]

    # Convert results to string for pattern matching
    results_str = str(results)

    # Look for high goal values
    for pattern in goal_patterns:
        matches = re.finditer(pattern, results_str, re.IGNORECASE)
        for match in matches:
            if match.groups():
                try:
                    goal_value = int(match.group(1))
                    if goal_value > max_goals:
                        print(f"⚠️ Found unrealistic goal value: {goal_value}")
                        return True
                except (ValueError, IndexError):
                    pass

    return False