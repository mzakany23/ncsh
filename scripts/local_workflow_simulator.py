#!/usr/bin/env python3
"""
Local Workflow Simulator for NC Soccer Scraper

This script simulates the AWS Step Function workflow locally by:
1. Calling the scraper Lambda function directly
2. Processing the results with the processing Lambda
3. Handling state transitions between steps

This allows full local testing of the workflow before deploying to AWS.
"""

import argparse
import json
import os
import sys
import importlib.util
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Set up paths for module imports
repo_root = Path(__file__).parent.parent.absolute()
# Add the scraping directory to the Python path
scraping_path = repo_root / "scraping"
sys.path.insert(0, str(scraping_path))
# Add the processing directory to the Python path if it exists
processing_path = repo_root / "processing"
if processing_path.exists():
    sys.path.insert(0, str(processing_path))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('workflow-simulator')

def import_module_from_file(file_path, module_name):
    """Import a module from a file path"""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module

def run_scraper_lambda(event):
    """Run the scraper Lambda function locally"""
    logger.info(f"Running scraper Lambda with event: {event}")
    
    # Find the Lambda function file
    repo_root = Path(__file__).parent.parent.absolute()
    lambda_file = repo_root / "scraping" / "lambda_function.py"
    
    if not lambda_file.exists():
        lambda_file = repo_root / "lambda_function.py"
        if not lambda_file.exists():
            raise FileNotFoundError(f"Could not find lambda_function.py in {repo_root}/scraping or {repo_root}")
    
    # Set up environment variables
    os.environ['LOCAL_EXECUTION'] = 'true'
    os.environ['DATA_DIR'] = str(repo_root / "data")
    
    # Create necessary directories
    data_dir = repo_root / "data"
    data_dir.mkdir(exist_ok=True)
    (data_dir / "html").mkdir(exist_ok=True)
    (data_dir / "json").mkdir(exist_ok=True)
    
    # Use direct import with updated path
    sys.path.insert(0, str(lambda_file.parent))
    
    try:
        # Import the lambda module dynamically
        spec = importlib.util.spec_from_file_location("lambda_function", lambda_file)
        lambda_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(lambda_module)
        
        # Run the Lambda handler
        result = lambda_module.lambda_handler(event, {})
        logger.info(f"Scraper Lambda completed with result: {result}")
        return result
    except Exception as e:
        logger.error(f"Error running scraper Lambda: {e}", exc_info=True)
        raise

def run_processing_lambda(event):
    """Run the processing Lambda function locally"""
    logger.info(f"Running processing Lambda with event: {event}")
    
    # Find the processing Lambda file
    repo_root = Path(__file__).parent.parent.absolute()
    processing_file = repo_root / "processing" / "lambda_function.py"
    
    if not processing_file.exists():
        processing_file = repo_root / "processing_lambda.py"
        if not processing_file.exists():
            logger.warning("Could not find processing Lambda function. Skipping processing step.")
            return {"status": "skipped", "reason": "Processing function not found"}
    
    # Set up environment variables
    os.environ['LOCAL_EXECUTION'] = 'true'
    os.environ['DATA_DIR'] = str(repo_root / "data")
    
    # Use direct import with updated path
    sys.path.insert(0, str(processing_file.parent))
    
    try:
        # Import the processing module dynamically
        spec = importlib.util.spec_from_file_location("processing_lambda", processing_file)
        processing_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(processing_module)
        
        # Run the Lambda handler
        result = processing_module.lambda_handler(event, {})
        logger.info(f"Processing Lambda completed with result: {result}")
        return result
    except Exception as e:
        logger.error(f"Error running processing Lambda: {e}", exc_info=True)
        raise

def simulate_workflow(config):
    """Simulate the complete Step Function workflow"""
    logger.info(f"Starting workflow simulation with config: {config}")
    
    # Initialize workflow state
    workflow_state = {
        "started_at": datetime.now().isoformat(),
        "steps": [],
        "result": None
    }
    
    try:
        # Step 1: Run the scraper
        scraper_event = {
            "year": config.get("year"),
            "month": config.get("month"),
            "mode": config.get("mode"),
            "force_scrape": config.get("force_scrape", True)
        }
        
        if config.get("mode") == "day" and config.get("day"):
            scraper_event["day"] = config.get("day")
        
        workflow_state["steps"].append({
            "name": "ScrapeSchedule",
            "started_at": datetime.now().isoformat(),
            "input": scraper_event
        })
        
        scraper_result = run_scraper_lambda(scraper_event)
        workflow_state["steps"][-1]["completed_at"] = datetime.now().isoformat()
        workflow_state["steps"][-1]["output"] = scraper_result
        
        # Step 2: Run the processing function if it exists
        processing_event = {
            "scraper_result": scraper_result,
            "year": config.get("year"),
            "month": config.get("month"),
            "mode": config.get("mode")
        }
        
        if config.get("mode") == "day" and config.get("day"):
            processing_event["day"] = config.get("day")
        
        workflow_state["steps"].append({
            "name": "ProcessData",
            "started_at": datetime.now().isoformat(),
            "input": processing_event
        })
        
        processing_result = run_processing_lambda(processing_event)
        workflow_state["steps"][-1]["completed_at"] = datetime.now().isoformat()
        workflow_state["steps"][-1]["output"] = processing_result
        
        # Complete workflow
        workflow_state["result"] = "SUCCESS"
        workflow_state["completed_at"] = datetime.now().isoformat()
        logger.info("Workflow completed successfully")
        
    except Exception as e:
        workflow_state["result"] = "FAILED"
        workflow_state["error"] = str(e)
        workflow_state["completed_at"] = datetime.now().isoformat()
        logger.error(f"Workflow failed: {e}", exc_info=True)
    
    # Save workflow state to file
    repo_root = Path(__file__).parent.parent.absolute()
    output_dir = repo_root / "test_output"
    output_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"workflow_simulation_{timestamp}.json"
    
    with open(output_file, 'w') as f:
        json.dump(workflow_state, f, indent=2)
    
    logger.info(f"Workflow state saved to {output_file}")
    return workflow_state

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Local Workflow Simulator for NC Soccer Scraper")
    parser.add_argument("--year", type=int, required=True, help="Year to scrape")
    parser.add_argument("--month", type=int, required=True, help="Month to scrape")
    parser.add_argument("--day", type=int, help="Day to scrape (for day mode)")
    parser.add_argument("--mode", choices=["day", "month"], default="day", help="Scrape mode")
    parser.add_argument("--force-scrape", action="store_true", help="Force scrape even if data exists")
    
    args = parser.parse_args()
    
    # Convert args to dict
    config = vars(args)
    
    # Run the workflow simulation
    result = simulate_workflow(config)
    
    # Print final status
    if result["result"] == "SUCCESS":
        print("\n✅ Workflow completed successfully")
        print(f"Check the data directory for output files")
    else:
        print("\n❌ Workflow failed")
        print(f"Error: {result.get('error', 'Unknown error')}")
