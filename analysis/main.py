# FYI: got this code from https://www.youtube.com/watch?v=YAIJV48QlXc&t=1714s&ab_channel=IndyDevDan
# really interesting way to use the OpenAI API to generate a SQL query based on a user request

import os
import sys
import json
import argparse
import subprocess
import uuid
from typing import List, Optional, Dict, Any
from rich.console import Console
from rich.panel import Panel
import openai
from pydantic import BaseModel, Field
from openai import pydantic_function_tool

# Initialize rich console
console = Console()

# Default path to the Parquet data
DEFAULT_DB_PATH = "data/parquet/data.parquet"

# Global variables
DB_PATH = None
TEMP_DB = None

# Create our list of function tools from our pydantic models
class QueryResult(BaseModel):
    sql_query: str
    raw_result: str
    formatted_result: Optional[str] = None
    metadata: Dict[str, Any] = {}

class ListTablesArgs(BaseModel):
    reasoning: str = Field(
        ..., description="Explanation for listing tables relative to the user request"
    )


class DescribeTableArgs(BaseModel):
    reasoning: str = Field(..., description="Reason why the table schema is needed")
    table_name: str = Field(..., description="Name of the table to describe")


class SampleTableArgs(BaseModel):
    reasoning: str = Field(..., description="Explanation for sampling the table")
    table_name: str = Field(..., description="Name of the table to sample")
    row_sample_size: int = Field(
        ..., description="Number of rows to sample (aim for 3-5 rows)"
    )


class RunTestSQLQuery(BaseModel):
    reasoning: str = Field(..., description="Reason for testing this query")
    sql_query: str = Field(..., description="The SQL query to test")


class RunFinalSQLQuery(BaseModel):
    reasoning: str = Field(
        ...,
        description="Final explanation of how this query satisfies the user request",
    )
    sql_query: str = Field(..., description="The validated SQL query to run")


class AnalyzeTeamPerformance(BaseModel):
    reasoning: str = Field(..., description="Reason for analyzing this team's performance")
    team_name: str = Field(..., description="Name of the team to analyze")
    time_period: Optional[str] = Field(None, description="Optional time period to analyze (e.g., 'this month', 'last 5 games')")


class GetTeamStats(BaseModel):
    reasoning: str = Field(..., description="Reason for getting team statistics")
    team_name: str = Field(..., description="Name of the team to get stats for")


class FuzzyTeamNameMatch(BaseModel):
    reasoning: str = Field(..., description="Reason for fuzzy matching the team name")
    search_term: str = Field(..., description="The team name to search for")


class SummarizeOutput(BaseModel):
    reasoning: str = Field(
        ...,
        description="Reason for summarizing the output"
    )
    conversation_json: str = Field(
        ...,
        description="The full conversation history to analyze as a JSON string"
    )
    user_request: str = Field(
        ...,
        description="The original user request"
    )

    model_config = {
        "json_schema_extra": {
            "required": ["reasoning", "user_request"]
        }
    }


class ListAvailableTeams(BaseModel):
    reasoning: str
    search_term: Optional[str] = None


# Create tools list
tools = [
    pydantic_function_tool(ListTablesArgs),
    pydantic_function_tool(DescribeTableArgs),
    pydantic_function_tool(SampleTableArgs),
    pydantic_function_tool(RunTestSQLQuery),
    pydantic_function_tool(RunFinalSQLQuery),
    pydantic_function_tool(AnalyzeTeamPerformance),
    pydantic_function_tool(GetTeamStats),
    pydantic_function_tool(FuzzyTeamNameMatch),
    pydantic_function_tool(SummarizeOutput),
    pydantic_function_tool(ListAvailableTeams),
]

AGENT_PROMPT = """<purpose>
    You are a world-class expert at crafting precise DuckDB SQL queries.
    Your goal is to generate accurate queries that exactly match the user's data needs.
</purpose>

<instructions>
    <instruction>Use the provided tools to explore the database and construct the perfect query.</instruction>
    <instruction>Start by listing tables to understand what's available.</instruction>
    <instruction>Describe tables to understand their schema and columns.</instruction>
    <instruction>Sample tables to see actual data patterns.</instruction>
    <instruction>Test queries before finalizing them.</instruction>
    <instruction>Only call run_final_sql_query when you're confident the query is perfect.</instruction>
    <instruction>Be thorough but efficient with tool usage.</instruction>
    <instruction>If you find your run_test_sql_query tool call returns an error or won't satisfy the user request, try to fix the query or try a different query.</instruction>
    <instruction>Think step by step about what information you need.</instruction>
    <instruction>Be sure to specify every parameter for each tool call.</instruction>
    <instruction>Every tool call should have a reasoning parameter which gives you a place to explain why you are calling the tool.</instruction>
    <instruction>When searching for team names, use the fuzzy_match_team_name tool to find similar matches if the exact name isn't found.</instruction>
    <instruction>After gathering all information, use the summarize_output tool to provide a clear, concise summary of findings.</instruction>
</instructions>

<tools>
    <tool>
        <name>list_tables</name>
        <description>Returns list of available tables in database</description>
        <parameters>
            <parameter>
                <name>reasoning</name>
                <type>string</type>
                <description>Why we need to list tables relative to user request</description>
                <required>true</required>
            </parameter>
        </parameters>
    </tool>

    <tool>
        <name>describe_table</name>
        <description>Returns schema info for specified table</description>
        <parameters>
            <parameter>
                <name>reasoning</name>
                <type>string</type>
                <description>Why we need to describe this table</description>
                <required>true</required>
            </parameter>
            <parameter>
                <name>table_name</name>
                <type>string</type>
                <description>Name of table to describe</description>
                <required>true</required>
            </parameter>
        </parameters>
    </tool>

    <tool>
        <name>sample_table</name>
        <description>Returns sample rows from specified table, always specify row_sample_size</description>
        <parameters>
            <parameter>
                <name>reasoning</name>
                <type>string</type>
                <description>Why we need to sample this table</description>
                <required>true</required>
            </parameter>
            <parameter>
                <name>table_name</name>
                <type>string</type>
                <description>Name of table to sample</description>
                <required>true</required>
            </parameter>
            <parameter>
                <name>row_sample_size</name>
                <type>integer</type>
                <description>Number of rows to sample aim for 3-5 rows</description>
                <required>true</required>
            </parameter>
        </parameters>
    </tool>

    <tool>
        <name>run_test_sql_query</name>
        <description>Tests a SQL query and returns results (only visible to agent)</description>
        <parameters>
            <parameter>
                <name>reasoning</name>
                <type>string</type>
                <description>Why we're testing this specific query</description>
                <required>true</required>
            </parameter>
            <parameter>
                <name>sql_query</name>
                <type>string</type>
                <description>The SQL query to test</description>
                <required>true</required>
            </parameter>
        </parameters>
    </tool>

    <tool>
        <name>run_final_sql_query</name>
        <description>Runs the final validated SQL query and shows results to user</description>
        <parameters>
            <parameter>
                <name>reasoning</name>
                <type>string</type>
                <description>Final explanation of how query satisfies user request</description>
                <required>true</required>
            </parameter>
            <parameter>
                <name>sql_query</name>
                <type>string</type>
                <description>The validated SQL query to run</description>
                <required>true</required>
            </parameter>
        </parameters>
    </tool>

    <tool>
        <name>fuzzy_match_team_name</name>
        <description>Finds team names that closely match the search term using fuzzy matching</description>
        <parameters>
            <parameter>
                <name>reasoning</name>
                <type>string</type>
                <description>Why we need to find similar team names</description>
                <required>true</required>
            </parameter>
            <parameter>
                <name>search_term</name>
                <type>string</type>
                <description>The team name to search for</description>
                <required>true</required>
            </parameter>
        </parameters>
    </tool>

    <tool>
        <name>summarize_output</name>
        <description>Analyzes the conversation and provides a clear summary of findings</description>
        <parameters>
            <parameter>
                <name>reasoning</name>
                <type>string</type>
                <description>Why we're summarizing the output</description>
                <required>true</required>
            </parameter>
            <parameter>
                <name>conversation_json</name>
                <type>string</type>
                <description>The full conversation history to analyze as a JSON string</description>
                <required>true</required>
            </parameter>
            <parameter>
                <name>user_request</name>
                <type>string</type>
                <description>The original user request</description>
                <required>true</required>
            </parameter>
        </parameters>
    </tool>
</tools>

<user-request>
    {{user_request}}
</user-request>
"""


def list_tables(reasoning: str) -> List[str]:
    """Returns a list of tables in the database."""
    try:
        result = subprocess.run(
            f'duckdb {TEMP_DB} -c ".tables"',
            shell=True,
            text=True,
            capture_output=True,
        )
        console.log(f"[blue]Step: Exploring available tables - {reasoning}[/blue]")
        return [x for x in result.stdout.strip().split("\n") if x]
    except Exception as e:
        console.log(f"[red]Error listing tables: {str(e)}[/red]")
        return []


def describe_table(reasoning: str, table_name: str) -> str:
    """Returns schema information about the specified table."""
    try:
        result = subprocess.run(
            f'duckdb {TEMP_DB} -c "DESCRIBE {table_name};"',
            shell=True,
            text=True,
            capture_output=True,
        )
        console.log(f"[blue]Step: Analyzing '{table_name}' structure - {reasoning}[/blue]")
        return result.stdout
    except Exception as e:
        console.log(f"[red]Error describing table: {str(e)}[/red]")
        return ""


def sample_table(reasoning: str, table_name: str, row_sample_size: int) -> str:
    """Returns a sample of rows from the specified table."""
    try:
        result = subprocess.run(
            f'duckdb {TEMP_DB} -c "SELECT * FROM {table_name} LIMIT {row_sample_size};"',
            shell=True,
            text=True,
            capture_output=True,
        )
        console.log(f"[blue]Step: Sampling {row_sample_size} rows from '{table_name}' - {reasoning}[/blue]")
        return result.stdout
    except Exception as e:
        console.log(f"[red]Error sampling table: {str(e)}[/red]")
        return ""


def run_test_sql_query(reasoning: str, sql_query: str) -> str:
    """Executes a test SQL query and returns results."""
    try:
        result = subprocess.run(
            f'duckdb {TEMP_DB} -c "{sql_query}"',
            shell=True,
            text=True,
            capture_output=True,
        )
        console.log(f"[blue]Step: Testing query - {reasoning}[/blue]")
        return result.stdout
    except Exception as e:
        console.log(f"[red]Error running test query: {str(e)}[/red]")
        return str(e)


def run_final_sql_query(reasoning: str, sql_query: str) -> str:
    """Executes the final SQL query and returns results to user."""
    try:
        result = subprocess.run(
            f'duckdb {TEMP_DB} -c "{sql_query}"',
            shell=True,
            text=True,
            capture_output=True,
        )
        console.print(Panel(f"[green]Final Analysis[/green]\n{reasoning}"))
        return result.stdout
    except Exception as e:
        console.log(f"[red]Error running final query: {str(e)}[/red]")
        return str(e)


def analyze_team_performance(reasoning: str, team_name: str, time_period: Optional[str] = None) -> str:
    """Analyzes a team's performance over a specified time period."""
    try:
        # Build the SQL query based on the time period
        year_filter = ""
        if time_period and "last year" in time_period.lower():
            year_filter = "AND EXTRACT(year FROM date) = EXTRACT(year FROM CURRENT_DATE) - 1"
        elif time_period and "this year" in time_period.lower():
            year_filter = "AND EXTRACT(year FROM date) = EXTRACT(year FROM CURRENT_DATE)"
        elif time_period and "month" in time_period.lower():
            year_filter = "AND date >= date_trunc('month', current_date)"
        elif time_period and "last 5" in time_period.lower():
            year_filter = "LIMIT 5"

        query = f"""
        WITH team_games AS (
            SELECT
                date,
                CASE
                    WHEN home_team = '{team_name}' THEN 'home'
                    ELSE 'away'
                END as location,
                CASE
                    WHEN home_team = '{team_name}' THEN home_score
                    ELSE away_score
                END as team_score,
                CASE
                    WHEN home_team = '{team_name}' THEN away_score
                    ELSE home_score
                END as opponent_score,
                CASE
                    WHEN home_team = '{team_name}' THEN away_team
                    ELSE home_team
                END as opponent,
                league
            FROM games
            WHERE (home_team = '{team_name}' OR away_team = '{team_name}')
            {year_filter}
            ORDER BY date DESC
        )
        SELECT
            COUNT(*) as total_games,
            SUM(CASE WHEN team_score > opponent_score THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN team_score < opponent_score THEN 1 ELSE 0 END) as losses,
            SUM(CASE WHEN team_score = opponent_score THEN 1 ELSE 0 END) as draws,
            SUM(team_score) as total_goals_scored,
            SUM(opponent_score) as total_goals_conceded,
            ROUND(AVG(team_score), 2) as avg_goals_scored,
            ROUND(AVG(opponent_score), 2) as avg_goals_conceded,
            STRING_AGG(DISTINCT league, ', ') as leagues_played
        FROM team_games
        WHERE team_score IS NOT NULL AND opponent_score IS NOT NULL;
        """

        result = subprocess.run(
            f'duckdb {TEMP_DB} -c "{query}"',
            shell=True,
            text=True,
            capture_output=True,
        )
        console.log(f"[blue]Step: Analyzing {team_name}'s performance - {reasoning}[/blue]")
        return result.stdout
    except Exception as e:
        console.log(f"[red]Error analyzing team performance: {str(e)}[/red]")
        return str(e)


def get_team_stats(reasoning: str, team_name: str) -> str:
    """Gets detailed statistics for a specific team."""
    try:
        query = f"""
        WITH team_games AS (
            SELECT
                date,
                league,
                CASE
                    WHEN home_team = '{team_name}' THEN 'home'
                    ELSE 'away'
                END as location,
                CASE
                    WHEN home_team = '{team_name}' THEN home_score
                    ELSE away_score
                END as team_score,
                CASE
                    WHEN home_team = '{team_name}' THEN away_score
                    ELSE home_score
                END as opponent_score,
                CASE
                    WHEN home_team = '{team_name}' THEN away_team
                    ELSE home_team
                END as opponent
            FROM games
            WHERE (home_team = '{team_name}' OR away_team = '{team_name}')
            AND EXTRACT(year FROM date) = EXTRACT(year FROM CURRENT_DATE) - 1
        )
        SELECT
            league,
            COUNT(*) as games_played,
            SUM(CASE WHEN team_score > opponent_score THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN team_score < opponent_score THEN 1 ELSE 0 END) as losses,
            SUM(CASE WHEN team_score = opponent_score THEN 1 ELSE 0 END) as draws,
            SUM(team_score) as total_goals_scored,
            SUM(opponent_score) as total_goals_conceded,
            ROUND(AVG(team_score), 2) as avg_goals_scored,
            ROUND(AVG(opponent_score), 2) as avg_goals_conceded
        FROM team_games
        WHERE team_score IS NOT NULL AND opponent_score IS NOT NULL
        GROUP BY league
        ORDER BY games_played DESC;
        """

        result = subprocess.run(
            f'duckdb {TEMP_DB} -c "{query}"',
            shell=True,
            text=True,
            capture_output=True,
        )
        console.log(f"[blue]Step: Getting detailed stats for {team_name} - {reasoning}[/blue]")
        return result.stdout
    except Exception as e:
        console.log(f"[red]Error getting team stats: {str(e)}[/red]")
        return str(e)


def fuzzy_match_team_name(reasoning: str, search_term: str) -> str:
    """Performs fuzzy matching to find team names similar to the search term."""
    try:
        # Use DuckDB's string similarity functions to find potential matches
        query = f"""
        WITH all_teams AS (
            SELECT DISTINCT home_team as team_name FROM games
            UNION
            SELECT DISTINCT away_team FROM games
        ),
        similarity_scores AS (
            SELECT
                team_name,
                jarowinkler_similarity(LOWER(team_name), LOWER('{search_term}')) as similarity,
                levenshtein(LOWER(team_name), LOWER('{search_term}')) as edit_distance,
                (
                    SELECT COUNT(*)
                    FROM games
                    WHERE home_team = team_name OR away_team = team_name
                ) as total_games
            FROM all_teams
            WHERE jarowinkler_similarity(LOWER(team_name), LOWER('{search_term}')) > 0.3
               OR levenshtein(LOWER(team_name), LOWER('{search_term}')) <= 5
        )
        SELECT
            team_name,
            ROUND(similarity * 100, 2) as similarity_percentage,
            edit_distance,
            total_games
        FROM similarity_scores
        ORDER BY similarity DESC, total_games DESC
        LIMIT 10;
        """

        result = subprocess.run(
            f'duckdb {TEMP_DB} -c "{query}"',
            shell=True,
            text=True,
            capture_output=True,
        )

        if not result.stdout.strip():
            # If no matches found, get a sample of most active teams
            sample_query = """
            WITH team_games AS (
                SELECT team_name, COUNT(*) as games_played
                FROM (
                    SELECT home_team as team_name FROM games
                    UNION ALL
                    SELECT away_team FROM games
                ) t
                GROUP BY team_name
            )
            SELECT team_name, games_played
            FROM team_games
            ORDER BY games_played DESC
            LIMIT 5;
            """
            sample_result = subprocess.run(
                f'duckdb {TEMP_DB} -c "{sample_query}"',
                shell=True,
                text=True,
                capture_output=True,
            )
            return f"No teams found matching '{search_term}'. Here are the most active teams in the database:\n{sample_result.stdout}"

        console.log(f"[blue]Step: Found teams matching '{search_term}' - {reasoning}[/blue]")
        return result.stdout
    except Exception as e:
        console.log(f"[red]Error in fuzzy team matching: {str(e)}[/red]")
        return str(e)


def summarize_output(reasoning: str, conversation_json: str, user_request: str) -> str:
    """Uses GPT-4 to analyze the conversation history and provide a clear, concise summary."""
    try:
        # Create a prompt for GPT-4 to analyze the conversation
        summary_prompt = f"""
        Please analyze this conversation about soccer data and provide a clear, concise summary.

        Original Request: {user_request}

        Conversation History:
        {conversation_json}

        Please provide:
        1. A clear answer to the original request, including:
           - Overall record (wins-losses-draws)
           - Goals scored and conceded
           - Performance trends or notable achievements
           - Leagues participated in
        2. Key insights from the data
        3. Any important context or caveats
        4. If no exact matches were found for team names, explain what was searched for and suggest how the user might refine their search
        5. Include examples of actual team names from the database if available

        Format the response in a clear, user-friendly way, using bullet points and sections where appropriate.
        If showing statistics, present them in an easy-to-read format.
        """

        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": summary_prompt}],
            temperature=0.7,
        )

        summary = response.choices[0].message.content
        console.print(Panel(f"[green]Summary[/green]\n{summary}"))
        return summary
    except Exception as e:
        console.log(f"[red]Error summarizing output: {str(e)}[/red]")
        return str(e)


def list_available_teams(reasoning: str, search_term: Optional[str] = None) -> str:
    """Lists available teams in the database, optionally filtered by a search term."""
    try:
        if search_term:
            # First, log all teams for debugging
            debug_query = """
            WITH all_teams AS (
                SELECT DISTINCT home_team as team_name FROM games
                UNION
                SELECT DISTINCT away_team FROM games
            )
            SELECT team_name
            FROM all_teams
            ORDER BY team_name;
            """
            debug_result = subprocess.run(
                f'duckdb {TEMP_DB} -c "{debug_query}"',
                shell=True,
                text=True,
                capture_output=True,
            )
            console.log(f"[blue]Step: Available teams in database:[/blue]\n{debug_result.stdout}")

            # Use fuzzy matching if a search term is provided
            query = f"""
            WITH all_teams AS (
                SELECT DISTINCT home_team as team_name FROM games
                UNION
                SELECT DISTINCT away_team FROM games
            ),
            similarity_scores AS (
                SELECT
                    team_name,
                    jarowinkler_similarity(LOWER(team_name), LOWER('{search_term}')) as similarity,
                    levenshtein(LOWER(team_name), LOWER('{search_term}')) as edit_distance
                FROM all_teams
                WHERE jarowinkler_similarity(LOWER(team_name), LOWER('{search_term}')) > 0.3
                   OR levenshtein(LOWER(team_name), LOWER('{search_term}')) <= 5
            )
            SELECT
                team_name,
                ROUND(similarity * 100, 2) as similarity_percentage,
                edit_distance
            FROM similarity_scores
            ORDER BY similarity DESC, edit_distance ASC
            LIMIT 10;
            """
            console.log(f"[blue]Step: Searching for teams similar to '{search_term}' - {reasoning}[/blue]")
        else:
            # List all teams if no search term
            query = """
            WITH all_teams AS (
                SELECT DISTINCT home_team as team_name FROM games
                UNION
                SELECT DISTINCT away_team FROM games
            )
            SELECT
                team_name,
                (
                    SELECT COUNT(*)
                    FROM games
                    WHERE home_team = team_name OR away_team = team_name
                ) as total_games
            FROM all_teams
            ORDER BY total_games DESC
            LIMIT 25;
            """
            console.log(f"[blue]Step: Listing top 25 teams by games played - {reasoning}[/blue]")

        result = subprocess.run(
            f'duckdb {TEMP_DB} -c "{query}"',
            shell=True,
            text=True,
            capture_output=True,
        )

        if not result.stdout.strip():
            return "No teams found in the database."

        return result.stdout
    except Exception as e:
        console.log(f"[red]Error listing teams: {str(e)}[/red]")
        return str(e)


def process_query(prompt: str, compute_iterations: int = 10) -> QueryResult:
    """Processes a query and returns the result."""
    messages = [{"role": "user", "content": prompt}]
    final_results = []
    final_query = None
    conversation_history = []

    console.print("\n[blue]Starting query processing...[/blue]")
    console.print(f"[blue]Input: {prompt}[/blue]")

    for iteration in range(compute_iterations):
        try:
            # Create a descriptive step label based on the conversation history and current state
            step_label = "Initial Search"  # Default for first step
            if iteration > 0 and conversation_history:
                last_action = conversation_history[-1].get("action", "")
                if "Finding similar team names" in last_action:
                    step_label = "Team Name Refinement"
                elif "Discovering available" in last_action:
                    step_label = "Database Exploration"
                elif "Testing SQL query" in last_action:
                    step_label = "Query Validation"
                elif "Executing final analysis" in last_action:
                    step_label = "Final Analysis"
                elif "Calculating team performance" in last_action:
                    step_label = "Performance Analysis"
                elif "Gathering detailed team statistics" in last_action:
                    step_label = "Statistics Compilation"
                elif "Generating result summary" in last_action:
                    step_label = "Summary Generation"
                else:
                    step_label = f"Chain Step {iteration + 1}"

            console.print(f"\n[blue]Step {iteration + 1}/{compute_iterations} - {step_label}:[/blue]")
            console.print("[blue]→ Sending request to GPT-4 to determine next action...[/blue]")

            response = openai.chat.completions.create(
                model="gpt-4o",
                messages=messages,
                tools=tools,
                tool_choice="auto",
            )

            if response.choices:
                message = response.choices[0].message
                if message.tool_calls and len(message.tool_calls) > 0:
                    tool_call = message.tool_calls[0]
                    func_call = tool_call.function
                    func_name = func_call.name
                    func_args = json.loads(func_call.arguments)

                    # Create a more descriptive action based on the tool being used
                    action_description = {
                        "ListTablesArgs": "Discovering available database tables",
                        "DescribeTableArgs": "Examining table structure",
                        "SampleTableArgs": "Inspecting sample data",
                        "RunTestSQLQuery": "Testing SQL query",
                        "RunFinalSQLQuery": "Executing final analysis query",
                        "AnalyzeTeamPerformance": "Calculating team performance metrics",
                        "GetTeamStats": "Gathering detailed team statistics",
                        "ListAvailableTeams": "Searching for available teams",
                        "FuzzyTeamNameMatch": "Finding similar team names",
                        "SummarizeOutput": "Generating result summary"
                    }.get(func_name, "Executing operation")

                    console.print(f"[blue]→ Action: {action_description}[/blue]")
                    console.print(f"[blue]→ Details: {func_args.get('reasoning', 'No details provided')}[/blue]")

                    # Store the tool call in conversation history
                    conversation_history.append({
                        "tool": func_name,
                        "args": func_args,
                        "iteration": iteration,
                        "action": action_description
                    })

                    if func_name == "RunFinalSQLQuery":
                        console.print("[green]→ Executing final analysis query...[/green]")
                        final_query = func_args["sql_query"]
                        result = run_final_sql_query(**func_args)
                        if not result:
                            raise ValueError("Empty result from final SQL query")
                        final_results.append(result)

                        console.print("[green]→ Compiling comprehensive summary of findings...[/green]")
                        # After getting final results, summarize everything
                        summary = summarize_output(
                            reasoning="Providing a clear summary of all findings",
                            conversation_json=json.dumps(conversation_history, indent=2),
                            user_request=prompt.replace(AGENT_PROMPT, "").strip()
                        )

                        return QueryResult(
                            sql_query=final_query if final_query else "No SQL query was generated for this request.",
                            raw_result=final_results[0],
                            formatted_result=summary,
                            metadata={
                                "iterations": iteration + 1,
                                "conversation_history": conversation_history,
                                "status": "completed_with_final_query"
                            }
                        )
                    else:
                        # Execute other tool calls but don't break
                        result = None
                        if func_name == "ListTablesArgs":
                            result = list_tables(**func_args)
                        elif func_name == "DescribeTableArgs":
                            result = describe_table(**func_args)
                        elif func_name == "SampleTableArgs":
                            result = sample_table(**func_args)
                        elif func_name == "RunTestSQLQuery":
                            result = run_test_sql_query(**func_args)
                        elif func_name == "AnalyzeTeamPerformance":
                            result = analyze_team_performance(**func_args)
                        elif func_name == "GetTeamStats":
                            result = get_team_stats(**func_args)
                        elif func_name == "ListAvailableTeams":
                            result = list_available_teams(**func_args)
                        elif func_name == "SummarizeOutput":
                            result = summarize_output(**func_args)

                        if result is None:
                            raise ValueError(f"Tool {func_name} returned no result")

                        # Store the result in conversation history
                        conversation_history[-1]["result"] = str(result)

                        messages.extend([
                            {
                                "role": "assistant",
                                "tool_calls": [{"id": tool_call.id, "type": "function", "function": func_call}],
                            },
                            {
                                "role": "tool",
                                "tool_call_id": tool_call.id,
                                "content": json.dumps({"result": str(result)})
                            }
                        ])

        except Exception as e:
            console.print(f"[red]Error in iteration {iteration}: {str(e)}[/red]")
            if iteration == compute_iterations - 1:
                console.print("[yellow]Reached maximum iterations, generating summary of findings so far...[/yellow]")
                # If we've hit max iterations, try to provide a summary anyway
                try:
                    summary = summarize_output(
                        reasoning="Summarizing results after reaching maximum iterations",
                        conversation_json=json.dumps(conversation_history, indent=2),
                        user_request=prompt.replace(AGENT_PROMPT, "").strip()
                    )
                    return QueryResult(
                        sql_query=final_query if final_query else "No SQL query was generated for this request.",
                        raw_result="Maximum iterations reached without finding requested information.",
                        formatted_result=summary,
                        metadata={
                            "iterations": iteration + 1,
                            "conversation_history": conversation_history,
                            "status": "max_iterations_reached"
                        }
                    )
                except Exception as summarize_error:
                    raise ValueError("Failed to generate a valid query and summary after all iterations") from e
            continue

    console.print("[yellow]Completed all iterations without final query, generating summary...[/yellow]")
    # If we somehow get here without returning earlier, summarize and return
    summary = summarize_output(
        reasoning="Providing a clear summary of all findings",
        conversation_json=json.dumps(conversation_history, indent=2),
        user_request=prompt.replace(AGENT_PROMPT, "").strip()
    )

    return QueryResult(
        sql_query=final_query if final_query else "No SQL query was generated for this request.",
        raw_result=final_results[0] if final_results else "No final results produced.",
        formatted_result=summary,
        metadata={
            "iterations": iteration + 1,
            "conversation_history": conversation_history,
            "status": "completed_without_final_query"
        }
    )


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="DuckDB Agent for querying Parquet data using OpenAI API")
    parser.add_argument(
        "-d", "--db",
        default=DEFAULT_DB_PATH,
        help=f"Path to Parquet file (default: {DEFAULT_DB_PATH})"
    )
    parser.add_argument("-p", "--prompt", required=True, help="The user's request")
    parser.add_argument(
        "-c",
        "--compute",
        type=int,
        default=10,
        help="Maximum number of agent loops (default: 10)",
    )
    args = parser.parse_args()

    # Configure the API key
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    if not OPENAI_API_KEY:
        console.print(
            "[red]Error: OPENAI_API_KEY environment variable is not set[/red]"
        )
        console.print(
            "Please get your API key from https://platform.openai.com/api-keys"
        )
        console.print("Then set it with: export OPENAI_API_KEY='your-api-key-here'")
        sys.exit(1)

    openai.api_key = OPENAI_API_KEY

    # Set global variables
    global DB_PATH, TEMP_DB
    DB_PATH = args.db

    # Create a unique database file name
    TEMP_DB = f"temp_db_{uuid.uuid4().hex[:8]}.duckdb"

    # Initialize DuckDB with the Parquet file
    try:
        # Create a database and load the Parquet file
        # First, create the database
        result = subprocess.run(
            f'duckdb {TEMP_DB} -c "CREATE TABLE games AS SELECT * FROM read_parquet(\'{DB_PATH}\');"',
            shell=True,
            text=True,
            capture_output=True,
        )

        # Verify the data was loaded correctly
        verify_result = subprocess.run(
            f'duckdb {TEMP_DB} -c "SELECT COUNT(*) FROM games;"',
            shell=True,
            text=True,
            capture_output=True,
        )

        if result.returncode != 0 or verify_result.returncode != 0:
            console.print(f"[red]Error initializing database: {result.stderr or verify_result.stderr}[/red]")
            if os.path.exists(TEMP_DB):
                os.unlink(TEMP_DB)
            sys.exit(1)

        console.print(f"[green]Successfully loaded {verify_result.stdout.strip()} games into database[/green]")

    except Exception as e:
        console.print(f"[red]Error initializing database: {str(e)}[/red]")
        if os.path.exists(TEMP_DB):
            os.unlink(TEMP_DB)
        sys.exit(1)

    try:
        # Process the query
        result = process_query(args.prompt, args.compute)

        # Print the results
        if result.formatted_result:
            console.print("\n[green]Analysis Complete[/green]")
            console.print(result.formatted_result)
        else:
            console.print("\n[yellow]No formatted results available[/yellow]")
            if result.raw_result:
                console.print(result.raw_result)

    finally:
        # Clean up temporary database
        if os.path.exists(TEMP_DB):
            os.unlink(TEMP_DB)


if __name__ == "__main__":
    main()