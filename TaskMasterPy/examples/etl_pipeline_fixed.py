"""
ETL Pipeline using TaskMasterPy

This script demonstrates how to use TaskMasterPy to create an ETL pipeline
that extracts data from an API, transforms it, and loads it into a CSV file.
"""
import sys
import os

# Add the parent directory to the path so we can import the taskmaster package
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from taskmaster.core.workflow import Workflow
from taskmaster.core.runner import WorkflowRunner
from taskmaster.triggers.time_trigger import TimeTrigger
from taskmaster.actions.api import CallAPIAction
from taskmaster.actions.transform_data import NormalizeAction, AggregateAction
from taskmaster.actions.save_data import SaveCSVAction
from taskmaster.actions.notify import ConsoleNotifyAction

def create_etl_pipeline():
    """Create an ETL pipeline workflow."""
    # Create a workflow
    workflow = Workflow(
        name="API ETL Pipeline",
        description="Extract data from an API, transform it, and load it into a CSV file"
    )
    
    # Add a trigger (run once)
    trigger = TimeTrigger(
        name="Manual Trigger",
        config={"schedule_str": "every 1 hour"}
    )
    workflow.add_trigger(trigger)
    
    # Extract: Call API to get data
    extract_action = CallAPIAction(
        name="Extract Data from API",
        config={
            "url": "https://jsonplaceholder.typicode.com/posts",
            "method": "GET",
            "return_type": "dataframe"
        }
    )
    workflow.add_action(extract_action)
    
    # Transform: Normalize the data
    normalize_action = NormalizeAction(
        name="Normalize Data",
        config={
            "columns": ["userId", "id"],
            "method": "minmax"
        }
    )
    workflow.add_action(normalize_action)
    workflow.add_dependency(normalize_action, extract_action)
    
    # Transform: Aggregate the data
    aggregate_action = AggregateAction(
        name="Aggregate Data",
        config={
            "group_by": ["userId"],
            "aggregations": {
                "id": ["count"],
                "title": ["count"]
            },
            "reset_index": True
        }
    )
    workflow.add_action(aggregate_action)
    workflow.add_dependency(aggregate_action, normalize_action)
    
    # Load: Save to CSV
    load_action = SaveCSVAction(
        name="Load Data to CSV",
        config={
            "file_path": "./data/api_etl_result.csv",
            "index": False
        }
    )
    workflow.add_action(load_action)
    workflow.add_dependency(load_action, aggregate_action)
    
    # Notify: Send notification
    notify_action = ConsoleNotifyAction(
        name="Notify Completion",
        config={
            "message": "ETL pipeline completed successfully",
            "level": "success"
        }
    )
    workflow.add_action(notify_action)
    workflow.add_dependency(notify_action, load_action)
    
    return workflow

def run_etl_pipeline():
    """Run the ETL pipeline workflow."""
    # Create the workflow
    workflow = create_etl_pipeline()
    
    # Create a runner
    runner = WorkflowRunner()
    
    # Register the workflow
    runner.register_workflow(workflow)
    
    # Run the workflow
    print(f"Running ETL pipeline: {workflow.name}")
    runner.run_workflow_now(workflow.id)
    print("ETL pipeline completed")

if __name__ == "__main__":
    run_etl_pipeline()
