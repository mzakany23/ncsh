#!/usr/bin/env python3
"""
Utility script to monitor and troubleshoot backfill jobs.
"""

import argparse
import boto3
import json
import logging
import time
from datetime import datetime
import pandas as pd
from tabulate import tabulate

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def get_state_machine_arn(name='ncsoccer-backfill', region='us-east-2'):
    """Get the ARN of a state machine by name."""
    try:
        sfn = boto3.client('stepfunctions', region_name=region)
        response = sfn.list_state_machines()
        
        for machine in response['stateMachines']:
            if machine['name'] == name:
                return machine['stateMachineArn']
        
        # Handle pagination if needed
        next_token = response.get('nextToken')
        while next_token:
            response = sfn.list_state_machines(nextToken=next_token)
            for machine in response['stateMachines']:
                if machine['name'] == name:
                    return machine['stateMachineArn']
            next_token = response.get('nextToken')
        
        raise ValueError(f"State machine '{name}' not found")
    
    except Exception as e:
        logger.error(f"Error finding state machine: {str(e)}")
        raise

def list_executions(state_machine_arn, status=None, max_results=20, region='us-east-2'):
    """List executions of the state machine."""
    try:
        sfn = boto3.client('stepfunctions', region_name=region)
        params = {
            'stateMachineArn': state_machine_arn,
            'maxResults': max_results
        }
        
        if status:
            params['statusFilter'] = status
        
        response = sfn.list_executions(**params)
        return response['executions']
    
    except Exception as e:
        logger.error(f"Error listing executions: {str(e)}")
        return []

def get_execution_details(execution_arn, region='us-east-2'):
    """Get detailed information about a state machine execution."""
    try:
        sfn = boto3.client('stepfunctions', region_name=region)
        return sfn.describe_execution(executionArn=execution_arn)
    
    except Exception as e:
        logger.error(f"Error getting execution details: {str(e)}")
        return None

def get_execution_history(execution_arn, region='us-east-2'):
    """Get the event history of a state machine execution."""
    try:
        sfn = boto3.client('stepfunctions', region_name=region)
        response = sfn.get_execution_history(
            executionArn=execution_arn,
            maxResults=1000  # Adjust as needed
        )
        return response['events']
    
    except Exception as e:
        logger.error(f"Error getting execution history: {str(e)}")
        return []

def check_s3_files(bucket, prefix, region='us-east-2'):
    """Check for files in S3 bucket with the given prefix."""
    try:
        s3 = boto3.client('s3', region_name=region)
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=1000
        )
        
        if 'Contents' in response:
            return len(response['Contents'])
        return 0
    
    except Exception as e:
        logger.error(f"Error checking S3 files: {str(e)}")
        return -1

def format_duration(start_time, stop_time=None):
    """Calculate and format the duration of an execution."""
    if not stop_time:
        stop_time = datetime.now(start_time.tzinfo) if hasattr(start_time, 'tzinfo') and start_time.tzinfo else datetime.now()
    
    if isinstance(start_time, str):
        start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
    
    if isinstance(stop_time, str):
        stop_time = datetime.fromisoformat(stop_time.replace('Z', '+00:00'))
    
    # Ensure both times have the same timezone awareness
    if hasattr(start_time, 'tzinfo') and start_time.tzinfo and not (hasattr(stop_time, 'tzinfo') and stop_time.tzinfo):
        stop_time = stop_time.replace(tzinfo=start_time.tzinfo)
    elif hasattr(stop_time, 'tzinfo') and stop_time.tzinfo and not (hasattr(start_time, 'tzinfo') and start_time.tzinfo):
        start_time = start_time.replace(tzinfo=stop_time.tzinfo)
    
    duration = stop_time - start_time
    total_seconds = duration.total_seconds()
    minutes = int(total_seconds // 60)
    seconds = int(total_seconds % 60)
    return f"{minutes}m {seconds}s"

def display_executions(executions, verbose=False, region='us-east-2'):
    """Display information about state machine executions in a table."""
    if not executions:
        logger.info("No executions found")
        return
    
    data = []
    for exec_info in executions:
        start_time = exec_info['startDate']
        stop_time = exec_info.get('stopDate', datetime.now())
        duration = format_duration(start_time, stop_time)
        
        row = {
            'Name': exec_info['name'],
            'Status': exec_info['status'],
            'Started': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'Duration': duration
        }
        
        if verbose:
            details = get_execution_details(exec_info['executionArn'], region)
            if details:
                if 'error' in details:
                    row['Error'] = details['error']
                if 'cause' in details:
                    row['Cause'] = details['cause'][:100] + '...' if len(details.get('cause', '')) > 100 else details.get('cause', '')
        
        data.append(row)
    
    df = pd.DataFrame(data)
    print(tabulate(df, headers='keys', tablefmt='pretty'))

def monitor_executions(state_machine_arn, interval=60, count=5, region='us-east-2'):
    """Monitor executions of the state machine at regular intervals."""
    for i in range(count):
        if i > 0:
            logger.info(f"Sleeping for {interval} seconds...")
            time.sleep(interval)
        
        logger.info(f"Checking executions (iteration {i+1}/{count}):")
        executions = list_executions(state_machine_arn, region=region)
        display_executions(executions, verbose=True, region=region)
        
        # Check S3 files
        json_files = check_s3_files('ncsh-app-data', 'data/json/', region)
        parquet_files = check_s3_files('ncsh-app-data', 'data/parquet/', region)
        logger.info(f"S3 file count - JSON: {json_files}, Parquet: {parquet_files}")
        print("-" * 80)

def analyze_execution(execution_arn, region='us-east-2'):
    """Perform a detailed analysis of a specific execution."""
    logger.info(f"Analyzing execution: {execution_arn}")
    
    # Get execution details
    details = get_execution_details(execution_arn, region)
    if not details:
        logger.error("Failed to get execution details")
        return
    
    print("\n=== Execution Details ===")
    print(f"Name: {details['name']}")
    print(f"Status: {details['status']}")
    print(f"Started: {details['startDate'].strftime('%Y-%m-%d %H:%M:%S')}")
    if 'stopDate' in details:
        print(f"Stopped: {details['stopDate'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Duration: {format_duration(details['startDate'], details['stopDate'])}")
    
    if 'error' in details:
        print(f"Error: {details['error']}")
    if 'cause' in details:
        print(f"Cause: {details['cause']}")
    
    # Get execution history
    print("\n=== Execution History ===")
    history = get_execution_history(execution_arn, region)
    if not history:
        logger.error("Failed to get execution history")
        return
    
    # Group events by state
    states = {}
    for event in history:
        if event['type'] == 'TaskStateEntered':
            state_name = event['stateEnteredEventDetails']['name']
            states[state_name] = {
                'entered': event['timestamp'],
                'events': [event]
            }
        elif event['type'] in ['TaskStateExited', 'TaskFailed']:
            prev_event = event.get('previousEventId')
            for state_name, state_info in states.items():
                if any(e['id'] == prev_event for e in state_info['events']):
                    state_info['events'].append(event)
                    if event['type'] == 'TaskStateExited':
                        state_info['exited'] = event['timestamp']
                    elif event['type'] == 'TaskFailed':
                        state_info['failed'] = event['timestamp']
                        if 'taskFailedEventDetails' in event:
                            state_info['error'] = event['taskFailedEventDetails'].get('error')
                            state_info['cause'] = event['taskFailedEventDetails'].get('cause')
    
    # Display state information
    state_data = []
    for state_name, state_info in states.items():
        entered = state_info.get('entered')
        exited = state_info.get('exited')
        failed = state_info.get('failed')
        
        status = "COMPLETED" if exited else "FAILED" if failed else "UNKNOWN"
        
        duration = None
        if entered and (exited or failed):
            end_time = exited if exited else failed
            duration = format_duration(entered, end_time)
        
        state_data.append({
            'State': state_name,
            'Status': status,
            'Duration': duration or 'N/A',
            'Error': state_info.get('error', 'N/A')
        })
    
    df = pd.DataFrame(state_data)
    print(tabulate(df, headers='keys', tablefmt='pretty'))
    
    # Detailed error analysis
    print("\n=== Error Analysis ===")
    for state_name, state_info in states.items():
        if 'error' in state_info:
            print(f"\nState '{state_name}' failed:")
            print(f"Error: {state_info.get('error', 'Unknown')}")
            
            # Try to parse and pretty-print the cause
            cause = state_info.get('cause', '')
            try:
                cause_json = json.loads(cause)
                print("Cause:")
                print(json.dumps(cause_json, indent=2))
            except (json.JSONDecodeError, TypeError):
                print(f"Cause: {cause}")

def main():
    parser = argparse.ArgumentParser(description='Monitor and troubleshoot NC Soccer backfill jobs')
    parser.add_argument('--state-machine', default='ncsoccer-backfill',
                      help='Name of the Step Function state machine')
    parser.add_argument('--region', default='us-east-2',
                      help='AWS region')
    parser.add_argument('--profile', help='AWS profile to use')
    parser.add_argument('--execution-arn', help='ARN of a specific execution to analyze')
    parser.add_argument('--monitor', action='store_true',
                      help='Continuously monitor executions')
    parser.add_argument('--interval', type=int, default=60,
                      help='Interval between monitoring checks (seconds)')
    parser.add_argument('--count', type=int, default=5,
                      help='Number of monitoring iterations')
    parser.add_argument('--verbose', '-v', action='store_true',
                      help='Show detailed information')
    
    args = parser.parse_args()
    
    # Configure AWS session
    if args.profile:
        boto3.setup_default_session(profile_name=args.profile, region_name=args.region)
    else:
        boto3.setup_default_session(region_name=args.region)
    
    try:
        if args.execution_arn:
            # Analyze a specific execution
            analyze_execution(args.execution_arn, args.region)
        else:
            # Get state machine ARN
            state_machine_arn = get_state_machine_arn(args.state_machine, args.region)
            logger.info(f"State machine ARN: {state_machine_arn}")
            
            if args.monitor:
                # Monitor executions
                monitor_executions(state_machine_arn, args.interval, args.count, args.region)
            else:
                # List and display executions
                executions = list_executions(state_machine_arn, max_results=20, region=args.region)
                display_executions(executions, args.verbose, args.region)
    
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return 1
    
    return 0

if __name__ == '__main__':
    exit(main())