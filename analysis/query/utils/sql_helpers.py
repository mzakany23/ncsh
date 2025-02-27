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

    # First, remove any markdown code blocks and explanatory text
    if '```' in sql_query:
        # Extract just the SQL part from markdown code blocks
        sql_parts = []
        in_code_block = False
        for line in sql_query.split('\n'):
            if line.strip().startswith('```'):
                in_code_block = not in_code_block
                continue
            if in_code_block and not line.startswith('#') and not line.startswith('--'):
                sql_parts.append(line)

        # If we found code blocks, join them
        if sql_parts:
            sql_query = '\n'.join(sql_parts)
        else:
            # Fallback: just strip out the markdown markers
            sql_query = re.sub(r'```sql\s*', '', sql_query)
            sql_query = re.sub(r'```\s*', '', sql_query)

    # Remove any explanatory text that might follow the SQL
    # Look for patterns like "This query will:" or numbered explanations
    explanation_patterns = [
        r'This query will:.*$',
        r'This SQL query will:.*$',
        r'\d+\.\s+.*$',  # Numbered explanations
        r'--\s+Explanation:.*$',
        r'/\*.*?\*/'  # SQL block comments
    ]

    for pattern in explanation_patterns:
        sql_query = re.sub(pattern, '', sql_query, flags=re.MULTILINE | re.DOTALL)

    # Check if the SQL starts with explanatory text (common with LLM responses)
    # If it doesn't start with common SQL keywords, try to find where the SQL actually starts
    sql_keywords = ['SELECT', 'WITH', 'CREATE', 'INSERT', 'UPDATE', 'DELETE', 'ALTER', 'DROP']
    if not any(sql_query.lstrip().upper().startswith(keyword) for keyword in sql_keywords):
        # Look for the first SQL keyword
        for keyword in sql_keywords:
            keyword_pos = sql_query.upper().find(keyword)
            if keyword_pos > 0:
                # Found a keyword - trim everything before it
                sql_query = sql_query[keyword_pos:]
                break

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

    # Final check - if the query is just explanatory text with no SQL, return a simple SELECT
    if not any(keyword in sql_query.upper() for keyword in sql_keywords):
        return "SELECT 'Error: No valid SQL found in response' AS error;"

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