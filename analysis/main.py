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


# Create tools list
tools = [
    pydantic_function_tool(ListTablesArgs),
    pydantic_function_tool(DescribeTableArgs),
    pydantic_function_tool(SampleTableArgs),
    pydantic_function_tool(RunTestSQLQuery),
    pydantic_function_tool(RunFinalSQLQuery),
    pydantic_function_tool(AnalyzeTeamPerformance),
    pydantic_function_tool(GetTeamStats),
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
</tools>

<user-request>
    {{user_request}}
</user-request>
"""


def list_tables(reasoning: str) -> List[str]:
    """Returns a list of tables in the database.

    The agent uses this to discover available tables and make informed decisions.

    Args:
        reasoning: Explanation of why we're listing tables relative to user request

    Returns:
        List of table names as strings
    """
    try:
        result = subprocess.run(
            f'duckdb {TEMP_DB} -c ".tables"',
            shell=True,
            text=True,
            capture_output=True,
        )
        console.log(f"[blue]List Tables Tool[/blue] - Reasoning: {reasoning}")
        return [x for x in result.stdout.strip().split("\n") if x]
    except Exception as e:
        console.log(f"[red]Error listing tables: {str(e)}[/red]")
        return []


def describe_table(reasoning: str, table_name: str) -> str:
    """Returns schema information about the specified table.

    The agent uses this to understand table structure and available columns.

    Args:
        reasoning: Explanation of why we're describing this table
        table_name: Name of table to describe

    Returns:
        String containing table schema information
    """
    try:
        result = subprocess.run(
            f'duckdb {TEMP_DB} -c "DESCRIBE {table_name};"',
            shell=True,
            text=True,
            capture_output=True,
        )
        console.log(
            f"[blue]Describe Table Tool[/blue] - Table: {table_name} - Reasoning: {reasoning}"
        )
        return result.stdout
    except Exception as e:
        console.log(f"[red]Error describing table: {str(e)}[/red]")
        return ""


def sample_table(reasoning: str, table_name: str, row_sample_size: int) -> str:
    """Returns a sample of rows from the specified table.

    The agent uses this to understand actual data content and patterns.

    Args:
        reasoning: Explanation of why we're sampling this table
        table_name: Name of table to sample from
        row_sample_size: Number of rows to sample aim for 3-5 rows

    Returns:
        String containing sample rows in readable format
    """
    try:
        result = subprocess.run(
            f'duckdb {TEMP_DB} -c "SELECT * FROM {table_name} LIMIT {row_sample_size};"',
            shell=True,
            text=True,
            capture_output=True,
        )
        console.log(
            f"[blue]Sample Table Tool[/blue] - Table: {table_name} - Rows: {row_sample_size} - Reasoning: {reasoning}"
        )
        return result.stdout
    except Exception as e:
        console.log(f"[red]Error sampling table: {str(e)}[/red]")
        return ""


def run_test_sql_query(reasoning: str, sql_query: str) -> str:
    """Executes a test SQL query and returns results.

    The agent uses this to validate queries before finalizing them.
    Results are only shown to the agent, not the user.

    Args:
        reasoning: Explanation of why we're running this test query
        sql_query: The SQL query to test

    Returns:
        Query results as a string
    """
    try:
        result = subprocess.run(
            f'duckdb {TEMP_DB} -c "{sql_query}"',
            shell=True,
            text=True,
            capture_output=True,
        )
        console.log(f"[blue]Test Query Tool[/blue] - Reasoning: {reasoning}")
        console.log(f"[dim]Query: {sql_query}[/dim]")
        return result.stdout
    except Exception as e:
        console.log(f"[red]Error running test query: {str(e)}[/red]")
        return str(e)


def run_final_sql_query(reasoning: str, sql_query: str) -> str:
    """Executes the final SQL query and returns results to user.

    This is the last tool call the agent should make after validating the query.

    Args:
        reasoning: Final explanation of how this query satisfies user request
        sql_query: The SQL query to run

    Returns:
        Query results as a string
    """
    try:
        result = subprocess.run(
            f'duckdb {TEMP_DB} -c "{sql_query}"',
            shell=True,
            text=True,
            capture_output=True,
        )
        console.log(
            Panel(
                f"[green]Final Query Tool[/green]\nReasoning: {reasoning}\nQuery: {sql_query}"
            )
        )
        return result.stdout
    except Exception as e:
        console.log(f"[red]Error running final query: {str(e)}[/red]")
        return str(e)


def analyze_team_performance(reasoning: str, team_name: str, time_period: Optional[str] = None) -> str:
    """Analyzes a team's performance over a specified time period.

    Args:
        reasoning: Explanation of why we're analyzing this team
        team_name: Name of the team to analyze
        time_period: Optional time period specification

    Returns:
        Analysis results as a string
    """
    try:
        # Build the SQL query based on the time period
        time_filter = ""
        if time_period:
            if "month" in time_period.lower():
                time_filter = "AND date >= date_trunc('month', current_date)"
            elif "last 5" in time_period.lower():
                time_filter = "LIMIT 5"

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
                END as opponent
            FROM games
            WHERE home_team = '{team_name}' OR away_team = '{team_name}'
            {time_filter}
            ORDER BY date DESC
        )
        SELECT
            COUNT(*) as total_games,
            SUM(CASE WHEN team_score > opponent_score THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN team_score < opponent_score THEN 1 ELSE 0 END) as losses,
            SUM(CASE WHEN team_score = opponent_score THEN 1 ELSE 0 END) as draws,
            AVG(team_score) as avg_goals_scored,
            AVG(opponent_score) as avg_goals_conceded
        FROM team_games;
        """

        result = subprocess.run(
            f'duckdb {TEMP_DB} -c "{query}"',
            shell=True,
            text=True,
            capture_output=True,
        )
        console.log(
            f"[blue]Team Analysis Tool[/blue] - Team: {team_name} - Reasoning: {reasoning}"
        )
        return result.stdout
    except Exception as e:
        console.log(f"[red]Error analyzing team performance: {str(e)}[/red]")
        return str(e)


def get_team_stats(reasoning: str, team_name: str) -> str:
    """Gets detailed statistics for a specific team.

    Args:
        reasoning: Explanation of why we need these stats
        team_name: Name of the team to get stats for

    Returns:
        Team statistics as a string
    """
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
            WHERE home_team = '{team_name}' OR away_team = '{team_name}'
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
        GROUP BY league
        ORDER BY games_played DESC;
        """

        result = subprocess.run(
            f'duckdb {TEMP_DB} -c "{query}"',
            shell=True,
            text=True,
            capture_output=True,
        )
        console.log(
            f"[blue]Team Stats Tool[/blue] - Team: {team_name} - Reasoning: {reasoning}"
        )
        return result.stdout
    except Exception as e:
        console.log(f"[red]Error getting team stats: {str(e)}[/red]")
        return str(e)


def process_query(prompt: str, compute_iterations: int = 10) -> QueryResult:
    """Processes a query and returns the result."""
    messages = [{"role": "user", "content": prompt}]
    final_results = []
    final_query = None

    for iteration in range(compute_iterations):
        try:
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

                    if func_name == "RunFinalSQLQuery":
                        final_query = func_args["sql_query"]
                        result = run_final_sql_query(**func_args)
                        if not result:
                            raise ValueError("Empty result from final SQL query")
                        final_results.append(result)
                        break
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

                        if result is None:
                            raise ValueError(f"Tool {func_name} returned no result")

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
                raise ValueError("Failed to generate a valid query after all iterations") from e
            continue

    if not final_results:
        raise ValueError("No final query result produced")

    return QueryResult(
        sql_query=final_query,
        raw_result=final_results[0],
        metadata={"iterations": iteration + 1}
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
        result = subprocess.run(
            f'duckdb {TEMP_DB} -c "CREATE TABLE games AS SELECT * FROM read_parquet(\'{DB_PATH}\');"',
            shell=True,
            text=True,
            capture_output=True,
        )
        if result.returncode != 0:
            console.print(f"[red]Error initializing database: {result.stderr}[/red]")
            if os.path.exists(TEMP_DB):
                os.unlink(TEMP_DB)
            sys.exit(1)
    except Exception as e:
        console.print(f"[red]Error initializing database: {str(e)}[/red]")
        if os.path.exists(TEMP_DB):
            os.unlink(TEMP_DB)
        sys.exit(1)

    try:
        # Create a single combined prompt based on the full template
        completed_prompt = AGENT_PROMPT.replace("{{user_request}}", args.prompt)
        messages = [{"role": "user", "content": completed_prompt}]

        compute_iterations = 0
        retry_count = 0
        MAX_RETRIES = 3

        # Main agent loop
        while True:
            console.rule(
                f"[yellow]Agent Loop {compute_iterations+1}/{args.compute} (Retry {retry_count}/{MAX_RETRIES})[/yellow]"
            )
            compute_iterations += 1

            if compute_iterations >= args.compute:
                console.print(
                    "[yellow]Warning: Reached maximum compute loops without final query[/yellow]"
                )
                raise Exception(
                    f"Maximum compute loops reached: {compute_iterations}/{args.compute}"
                )

            try:
                # Generate content with tool support
                response = openai.chat.completions.create(
                    model="gpt-4o",
                    messages=messages,
                    tools=tools,
                    tool_choice="auto",
                )

                if response.choices:
                    assert len(response.choices) == 1
                    message = response.choices[0].message

                    if message.function_call:
                        func_call = message.function_call
                    elif message.tool_calls and len(message.tool_calls) > 0:
                        # If a tool_calls list is present, use the first call and extract its function details.
                        tool_call = message.tool_calls[0]
                        func_call = tool_call.function
                    else:
                        func_call = None

                    if func_call:
                        func_name = func_call.name
                        func_args_str = func_call.arguments

                        messages.append(
                            {
                                "role": "assistant",
                                "tool_calls": [
                                    {
                                        "id": tool_call.id,
                                        "type": "function",
                                        "function": func_call,
                                    }
                                ],
                            }
                        )

                        console.print(
                            f"[blue]Function Call:[/blue] {func_name}({func_args_str})"
                        )
                        try:
                            # Validate and parse arguments using the corresponding pydantic model
                            if func_name == "ListTablesArgs":
                                args_parsed = ListTablesArgs.model_validate_json(
                                    func_args_str
                                )
                                result = list_tables(reasoning=args_parsed.reasoning)
                            elif func_name == "DescribeTableArgs":
                                args_parsed = DescribeTableArgs.model_validate_json(
                                    func_args_str
                                )
                                result = describe_table(
                                    reasoning=args_parsed.reasoning,
                                    table_name=args_parsed.table_name,
                                )
                            elif func_name == "SampleTableArgs":
                                args_parsed = SampleTableArgs.model_validate_json(
                                    func_args_str
                                )
                                result = sample_table(
                                    reasoning=args_parsed.reasoning,
                                    table_name=args_parsed.table_name,
                                    row_sample_size=args_parsed.row_sample_size,
                                )
                            elif func_name == "RunTestSQLQuery":
                                args_parsed = RunTestSQLQuery.model_validate_json(
                                    func_args_str
                                )
                                result = run_test_sql_query(
                                    reasoning=args_parsed.reasoning,
                                    sql_query=args_parsed.sql_query,
                                )
                            elif func_name == "RunFinalSQLQuery":
                                args_parsed = RunFinalSQLQuery.model_validate_json(
                                    func_args_str
                                )
                                result = run_final_sql_query(
                                    reasoning=args_parsed.reasoning,
                                    sql_query=args_parsed.sql_query,
                                )
                                console.print("\n[green]Final Results:[/green]")
                                console.print(result)
                                return
                            elif func_name == "AnalyzeTeamPerformance":
                                args_parsed = AnalyzeTeamPerformance.model_validate_json(
                                    func_args_str
                                )
                                result = analyze_team_performance(
                                    reasoning=args_parsed.reasoning,
                                    team_name=args_parsed.team_name,
                                    time_period=args_parsed.time_period,
                                )
                            elif func_name == "GetTeamStats":
                                args_parsed = GetTeamStats.model_validate_json(
                                    func_args_str
                                )
                                result = get_team_stats(
                                    reasoning=args_parsed.reasoning,
                                    team_name=args_parsed.team_name,
                                )
                            else:
                                raise Exception(f"Unknown tool call: {func_name}")

                            console.print(
                                f"[blue]Function Call Result:[/blue] {func_name}(...) ->\n{result}"
                            )

                            # Reset retry count on successful function call
                            retry_count = 0

                            # Append the function call result into our messages as a tool response
                            messages.append(
                                {
                                    "role": "tool",
                                    "tool_call_id": tool_call.id,
                                    "content": json.dumps({"result": str(result)}),
                                }
                            )

                        except Exception as e:
                            error_msg = f"Argument validation failed for {func_name}: {e}"
                            console.print(f"[red]{error_msg}[/red]")
                            messages.append(
                                {
                                    "role": "tool",
                                    "tool_call_id": tool_call.id,
                                    "content": json.dumps({"error": error_msg}),
                                }
                            )
                            retry_count += 1
                            if retry_count >= MAX_RETRIES:
                                raise Exception(f"Maximum retries ({MAX_RETRIES}) reached")
                            continue
                    else:
                        # No function call in response, try to recover
                        retry_count += 1
                        if retry_count >= MAX_RETRIES:
                            raise Exception(f"Maximum retries ({MAX_RETRIES}) reached")

                        # Add a recovery message to guide the model
                        messages.append({
                            "role": "user",
                            "content": "Please use one of the available tools to help answer the question. "
                                     "You can list tables, describe them, analyze team performance, or get team statistics."
                        })
                        continue

            except Exception as e:
                console.print(f"[red]Error in agent loop: {str(e)}[/red]")
                retry_count += 1
                if retry_count >= MAX_RETRIES:
                    raise Exception(f"Maximum retries ({MAX_RETRIES}) reached")
                continue
    finally:
        # Clean up temporary database
        if os.path.exists(TEMP_DB):
            os.unlink(TEMP_DB)


if __name__ == "__main__":
    main()