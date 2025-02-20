import os
import json
import subprocess
import uuid
from typing import Dict, Any, Optional, List, Union
import boto3
from main import (
    list_tables,
    describe_table,
    sample_table,
    run_test_sql_query,
    run_final_sql_query,
    analyze_team_performance,
    get_team_stats,
    tools,
)
import openai
from rich.console import Console
from pydantic import BaseModel, Field

# Initialize rich console
console = Console()

# AWS clients
s3 = boto3.client('s3')
api_client = boto3.client('apigatewaymanagementapi')
stepfunctions = boto3.client('stepfunctions')

class QueryRequest(BaseModel):
    prompt: str
    compute_iterations: Optional[int] = 10
    mode: str = Field(default="sync", description="Either 'sync' or 'stream'")
    format_type: Optional[str] = Field(default="default", description="How to format the response")

class QueryResult(BaseModel):
    sql_query: str
    raw_result: str
    formatted_result: Optional[str] = None
    metadata: Dict[str, Any] = {}

def format_query_result(result: str, format_type: str, prompt: str) -> str:
    """Formats the query result using GPT-4 based on the format type and original prompt."""
    try:
        format_prompt = f"""Format the following DuckDB query result for a user who asked: "{prompt}"

Format type requested: {format_type}

Query result:
{result}

Please format this data in a clear, concise way that directly answers the user's question.
If the format_type is 'default', use a natural language response with relevant numbers and statistics.
If it's 'table', format as a markdown table.
If it's 'summary', provide a brief executive summary.
If it's 'analysis', provide a detailed analysis with insights.

Response:"""

        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": format_prompt}],
            temperature=0.7,
        )

        return response.choices[0].message.content
    except Exception as e:
        console.print(f"[red]Error formatting result: {str(e)}[/red]")
        return result

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
                        final_results.append(result)
                        break
                    else:
                        # Execute other tool calls but don't break
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
            continue

    if not final_results:
        raise Exception("No final query result produced")

    return QueryResult(
        sql_query=final_query,
        raw_result=final_results[0],
        metadata={"iterations": iteration + 1}
    )

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda handler supporting both sync and streaming modes."""
    try:
        # Check if this is a Step Function execution
        if 'step' in event:
            step = event['step']

            if step == 'query':
                # Process the query and return the SQL result
                result = process_query(
                    prompt=event['prompt'],
                    compute_iterations=event.get('compute_iterations', 10)
                )
                return {
                    'statusCode': 200,
                    'result': result.dict()
                }

            elif step == 'format':
                # Format the query result
                formatted_result = format_query_result(
                    result=event['result']['raw_result'],
                    format_type=event.get('format_type', 'default'),
                    prompt=event['prompt']
                )
                return {
                    'statusCode': 200,
                    'result': {
                        **event['result'],
                        'formatted_result': formatted_result
                    }
                }

            else:
                raise ValueError(f"Unknown step: {step}")

        # Handle WebSocket or synchronous API requests
        request = QueryRequest(**json.loads(event.get('body', '{}')))

        if request.mode == 'stream':
            # Extract WebSocket details
            connection_id = event['requestContext']['connectionId']
            domain_name = event['requestContext']['domainName']
            stage = event['requestContext']['stage']
            endpoint_url = f'https://{domain_name}/{stage}'

            # Start step function execution
            execution = stepfunctions.start_execution(
                stateMachineArn=os.environ['STEP_FUNCTION_ARN'],
                input=json.dumps({
                    'connection_id': connection_id,
                    'endpoint_url': endpoint_url,
                    'prompt': request.prompt,
                    'compute_iterations': request.compute_iterations,
                    'format_type': request.format_type,
                    'mode': 'stream'
                })
            )

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Processing started',
                    'execution_arn': execution.get('executionArn')
                })
            }

        else:  # Synchronous mode
            # Process query
            result = process_query(
                prompt=request.prompt,
                compute_iterations=request.compute_iterations
            )

            # Format result
            formatted_result = format_query_result(
                result=result.raw_result,
                format_type=request.format_type,
                prompt=request.prompt
            )

            result.formatted_result = formatted_result

            return {
                'statusCode': 200,
                'body': json.dumps(result.dict())
            }

    except Exception as e:
        error_response = {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
        return error_response