#!/usr/bin/env python3
import os
import sys
import json
import time
import argparse
import boto3
from rich.console import Console
from rich.panel import Panel

console = Console()

def submit_query(step_function_arn, prompt, format_type='default'):
    """Submit a query to the analysis Step Function."""
    sfn = boto3.client('stepfunctions', region_name='us-east-2')

    # Prepare input for the Step Function
    input_data = {
        "prompt": prompt,
        "format_type": format_type
    }

    # Start execution
    response = sfn.start_execution(
        stateMachineArn=step_function_arn,
        input=json.dumps(input_data)
    )

    return response['executionArn']

def check_execution(execution_arn):
    """Check the status of a Step Function execution."""
    sfn = boto3.client('stepfunctions', region_name='us-east-2')
    response = sfn.describe_execution(executionArn=execution_arn)
    return response

def poll_until_complete(execution_arn, max_attempts=30, delay=2):
    """Poll the execution until it completes or fails."""
    attempts = 0
    while attempts < max_attempts:
        response = check_execution(execution_arn)
        status = response['status']

        if status == 'SUCCEEDED':
            try:
                result = json.loads(response['output'])
                if result.get('statusCode') == 200:
                    return result['result']
                else:
                    raise Exception(f"Error in result: {result.get('error', 'Unknown error')}")
            except json.JSONDecodeError as e:
                raise Exception(f"Error parsing result: {e}")
        elif status in ['FAILED', 'TIMED_OUT', 'ABORTED']:
            error_output = json.loads(response.get('output', '{}'))
            error_msg = error_output.get('error', 'Unknown error')
            raise Exception(f"Execution failed: {error_msg}")

        attempts += 1
        if attempts < max_attempts:
            time.sleep(delay)

    raise Exception("Polling timed out")

def main():
    parser = argparse.ArgumentParser(description='Query the NC Soccer analysis Step Function')
    parser.add_argument('-p', '--prompt', required=True, help='The query prompt')
    parser.add_argument(
        '-f', '--format',
        choices=['default', 'table', 'summary', 'analysis'],
        default='default',
        help='Output format type'
    )
    parser.add_argument(
        '--profile',
        default='mzakany',
        help='AWS profile to use'
    )

    args = parser.parse_args()

    # Set AWS profile
    os.environ['AWS_PROFILE'] = args.profile

    try:
        # Submit query
        console.print("[blue]Submitting query...[/blue]")
        step_function_arn = "arn:aws:states:us-east-2:552336166511:stateMachine:ncsoccer-analysis"
        execution_arn = submit_query(step_function_arn, args.prompt, args.format)
        console.print(f"[blue]Query started. Execution ARN: {execution_arn}[/blue]")

        # Poll for results
        console.print("[blue]Waiting for results...[/blue]")
        result = poll_until_complete(execution_arn)

        # Display results
        if result.get('formatted_result'):
            console.print(Panel(
                str(result['formatted_result']),
                title="[green]Analysis Result[/green]",
                expand=False
            ))
        else:
            console.print(Panel(
                str(result['raw_result']),
                title="[green]Raw Result[/green]",
                expand=False
            ))

        # Display SQL query if requested
        if result.get('sql_query'):
            console.print(Panel(
                result['sql_query'],
                title="[blue]SQL Query[/blue]",
                expand=False
            ))

    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        sys.exit(1)

if __name__ == '__main__':
    main()