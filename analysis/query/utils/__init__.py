"""
Utility functions for the Query Engine.
"""

from .sql_helpers import fix_duckdb_sql, has_balanced_parentheses, fix_unbalanced_parentheses
from .validation import is_empty_result, has_unrealistic_values
from .team_info import get_all_teams, find_best_matching_team, get_teams_by_division, get_available_divisions

__all__ = [
    # SQL Helpers
    'fix_duckdb_sql',
    'has_balanced_parentheses',
    'fix_unbalanced_parentheses',

    # Validation
    'is_empty_result',
    'has_unrealistic_values',

    # Team Info
    'get_all_teams',
    'find_best_matching_team',
    'get_teams_by_division',
    'get_available_divisions',
]