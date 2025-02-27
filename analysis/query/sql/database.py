"""SQL database adapter for the Query Engine."""

import re
from llama_index.core.utilities.sql_wrapper import SQLDatabase
from ..utils.sql_helpers import fix_duckdb_sql

class DuckDBSQLDatabase(SQLDatabase):
    """
    Custom DuckDB SQL database with special handling for DuckDB's syntax requirements.
    """

    def run_sql(self, sql_query: str, **kwargs):
        """
        Execute a SQL query with DuckDB-specific fixes applied.

        Args:
            sql_query (str): SQL query string to execute
            **kwargs: Additional arguments passed to the parent's run_sql

        Returns:
            The result of the SQL query
        """
        # Fix DuckDB-specific syntax issues
        fixed_query = fix_duckdb_sql(sql_query)

        print(f"🔍 Executing SQL: {fixed_query}")

        # Call the parent implementation with the fixed query
        return super().run_sql(fixed_query, **kwargs)