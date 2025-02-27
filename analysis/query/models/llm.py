"""LLM utilities for the Query Engine.

This module provides functions to set up and configure the language models
used for SQL generation and response formatting.
"""

import os
from typing import Optional, Dict, Any, List

from llama_index.llms.anthropic import Anthropic

def get_llm(
    model_name: str = "claude-3-7-sonnet-latest",
    api_key: Optional[str] = None,
    api_base: Optional[str] = None,
    temperature: float = 0.0,
    max_tokens: int = 4000
) -> Anthropic:
    """
    Get a language model instance configured for the query engine.

    Args:
        model_name: Name of the Anthropic model to use (default: claude-3-7-sonnet-latest)
        api_key: Anthropic API key
        api_base: API base URL (rarely needed for Anthropic)
        temperature: Temperature for generation (lower for more deterministic outputs)
        max_tokens: Maximum number of tokens to generate in the response

    Returns:
        Configured LLM instance
    """
    # Use provided API key or get from environment
    anthropic_api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")

    if not anthropic_api_key:
        print("⚠️ Warning: No Anthropic API key provided. Falling back to environment variable.")

    # Create Anthropic LLM instance using LlamaIndex
    llm = Anthropic(
        api_key=anthropic_api_key,
        model=model_name,
        temperature=temperature,
        max_tokens=max_tokens
    )

    # Set API base URL if provided (uncommon for Anthropic)
    if api_base:
        llm.api_url = api_base

    return llm