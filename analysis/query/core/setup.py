"""Setup utilities for the Query Engine.

This module provides functions to set up and initialize the query engine,
including database connection, LLM configuration, and query execution.
"""

import os
import logging
from typing import Dict, List, Any, Optional, Union

from llama_index.core.indices.struct_store import NLSQLTableQueryEngine
from llama_index.core.response import Response

from ..models.llm import get_llm
from ..sql.database import DuckDBSQLDatabase
from .engine import QueryEngine


def setup_query_engine(
    db_path: str = "matches.parquet",
    model_name: str = "claude-3-7-sonnet-latest",
    api_key: Optional[str] = None,
    api_base: Optional[str] = None,
    temperature: float = 0.0,
    verbose: bool = False,
    always_infer: bool = False,
) -> QueryEngine:
    """
    Set up and initialize the QueryEngine with database and LLM configuration.

    Args:
        db_path: Path to the database file (default: matches.parquet)
        model_name: Name of the LLM model to use (default: claude-3-7-sonnet-latest)
        api_key: API key for Anthropic
        api_base: API base URL for Anthropic (optional)
        temperature: Temperature for LLM generation (default: 0.0)
        verbose: Whether to enable verbose logging (default: False)
        always_infer: Always use LLM to infer query instead of templates (default: False)

    Returns:
        Initialized QueryEngine instance
    """
    # Configure logging
    log_level = logging.INFO if verbose else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    # Validate file path
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file not found: {db_path}")

    # Initialize LLM
    logger.info(f"Initializing LLM with model: {model_name}")
    llm = get_llm(
        model_name=model_name,
        api_key=api_key,
        api_base=api_base,
        temperature=temperature,
    )

    # Initialize SQL database
    logger.info(f"Initializing DuckDB database with file: {db_path}")

    # Create an in-memory DuckDB engine
    from sqlalchemy import create_engine, text
    engine = create_engine("duckdb:///:memory:", future=True)

    # Load the data from parquet file into the matches table
    with engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS matches AS SELECT * FROM read_parquet('{db_path}')"))
        # Verify table exists and has data
        result = conn.execute(text("SELECT COUNT(*) FROM matches")).scalar()
        logger.info(f"Loaded {result} matches into database")

    # Initialize DuckDB database with the engine
    sql_database = DuckDBSQLDatabase(
        engine=engine,
        include_tables=["matches"],
    )

    # Initialize query engine
    logger.info("Initializing QueryEngine")
    query_engine = QueryEngine(
        sql_database=sql_database,
        llm=llm,
        always_infer=always_infer,
    )

    logger.info("QueryEngine setup complete")
    return query_engine


def run_query(
    query: str,
    engine: Optional[QueryEngine] = None,
    db_path: str = "matches.parquet",
    model_name: str = "claude-3-7-sonnet-latest",
    api_key: Optional[str] = None,
    api_base: Optional[str] = None,
    temperature: float = 0.0,
    memory: Optional[Any] = None,
    verbose: bool = False,
    always_infer: bool = False,
) -> str:
    """
    Run a query using the QueryEngine.

    If an engine is not provided, one will be created with the specified parameters.

    Args:
        query: The natural language query to process
        engine: Optional existing QueryEngine instance
        db_path: Path to the database file (if engine not provided)
        model_name: Name of the LLM model to use (if engine not provided)
        api_key: API key for Anthropic (if engine not provided)
        api_base: API base URL for Anthropic (if engine not provided)
        temperature: Temperature for LLM generation (if engine not provided)
        memory: Optional memory context for conversation history
        verbose: Whether to enable verbose logging
        always_infer: Always use LLM to infer query instead of templates (default: False)

    Returns:
        Natural language response to the query
    """
    # Configure logging
    log_level = logging.INFO if verbose else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    # Create engine if not provided
    if engine is None:
        logger.info("Creating new QueryEngine instance")
        engine = setup_query_engine(
            db_path=db_path,
            model_name=model_name,
            api_key=api_key,
            api_base=api_base,
            temperature=temperature,
            verbose=verbose,
            always_infer=always_infer,
        )

    # Get session ID from environment if present
    session_id = os.environ.get("QUERY_SESSION")
    if session_id and memory and not hasattr(memory, 'session_id'):
        memory.session_id = session_id
        logger.info(f"Using session ID from environment: {session_id}")

    # Run the query
    logger.info(f"Running query: {query}")
    response = engine.query(query, memory=memory)

    return response