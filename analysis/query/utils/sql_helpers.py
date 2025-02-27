"""SQL helper utilities for the Query Engine."""

import re

def fix_duckdb_sql(sql_query: str) -> str:
    """
    Fix SQL syntax issues specific to DuckDB.

    Args:
        sql_query: The SQL query to fix

    Returns:
        str: The fixed SQL query
    """
    # Ensure we have a string
    if not isinstance(sql_query, str):
        return sql_query

    # First, remove any markdown code blocks
    if sql_query.startswith('```sql') or sql_query.startswith('```'):
        sql_query = re.sub(r'^```sql\n', '', sql_query)
        sql_query = re.sub(r'^```\n', '', sql_query)
        sql_query = re.sub(r'\n```$', '', sql_query)

    # Remove trailing comments that might have incomplete code
    sql_query = re.sub(r'--.*$', '', sql_query, flags=re.MULTILINE)

    # Fix trailing parentheses that might be unbalanced
    open_count = sql_query.count('(')
    close_count = sql_query.count(')')

    if open_count > close_count:
        # Add missing closing parentheses
        missing = open_count - close_count
        if sql_query.rstrip().endswith(';'):
            sql_query = sql_query.rstrip(';') + (')' * missing) + ';'
        else:
            sql_query = sql_query + (')' * missing)

    # Make sure semicolon is present at the end
    if not sql_query.rstrip().endswith(';'):
        sql_query = sql_query.rstrip() + ';'

    # Clean up any weird trailing commas before closing parentheses
    sql_query = re.sub(r',\s*\)', ')', sql_query)

    # Ensure clean whitespace
    sql_query = re.sub(r'\s+', ' ', sql_query)
    sql_query = re.sub(r'\s*;\s*$', ';', sql_query)

    return sql_query


def has_balanced_parentheses(sql_query: str) -> bool:
    """
    Check if a SQL query has balanced parentheses.

    Args:
        sql_query: The SQL query to check

    Returns:
        bool: True if parentheses are balanced, False otherwise
    """
    stack = []
    for char in sql_query:
        if char == '(':
            stack.append(char)
        elif char == ')':
            if not stack:  # More closing than opening
                return False
            stack.pop()

    return len(stack) == 0  # Should be empty if balanced


def fix_unbalanced_parentheses(sql_query: str) -> str:
    """
    Attempt to fix unbalanced parentheses in a SQL query.

    Args:
        sql_query: The SQL query to fix

    Returns:
        str: The fixed SQL query
    """
    # Count opening and closing parentheses
    open_count = sql_query.count('(')
    close_count = sql_query.count(')')

    if open_count > close_count:
        # Add missing closing parentheses
        missing = open_count - close_count
        if sql_query.rstrip().endswith(';'):
            sql_query = sql_query.rstrip(';') + (')' * missing) + ';'
        else:
            sql_query = sql_query + (')' * missing)
        print(f"⚠️ Added {missing} closing parentheses to balance the query.")
    elif close_count > open_count:
        # Remove excess closing parentheses (less common)
        # This is trickier - we'll try to identify and remove trailing excess parentheses
        excess = close_count - open_count
        last_semicolon = sql_query.rfind(';')
        if last_semicolon != -1:
            # Check for excess closing parentheses before the semicolon
            section_before_semicolon = sql_query[:last_semicolon]
            trailing_parens = re.search(r'\)+$', section_before_semicolon)
            if trailing_parens:
                # Remove the excess closing parentheses
                trailing_count = min(excess, len(trailing_parens.group(0)))
                sql_query = section_before_semicolon[:-trailing_count] + ';' + sql_query[last_semicolon+1:]
                print(f"⚠️ Removed {trailing_count} excess closing parentheses.")

    return sql_query