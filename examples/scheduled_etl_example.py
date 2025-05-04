"""
Scheduled ETL Example using TaskMasterPy

This script demonstrates how to use TaskMasterPy to:
1. Schedule an ETL process to run at regular intervals
2. Extract data from a CSV file
3. Transform the data
4. Load the data into a new CSV file

Author: Shatrugna
"""

import time

from taskmaster.core.workflow import Workflow
from taskmaster.core.runner import WorkflowRunner
from taskmaster.triggers.time_trigger import TimeTrigger
from taskmaster.actions.load_data import LoadCSVAction
from taskmaster.actions.clean_data import DropNAAction, FixDataTypesAction
from taskmaster.actions.transform_data import AggregateAction
from taskmaster.actions.save_data import SaveCSVAction
from taskmaster.actions.notify import ConsoleNotifyAction

def main():
    # Create a workflow
    workflow = Workflow(name="Scheduled ETL Workflow", description="ETL process that runs on a schedule")
    
    # Add a time trigger to run the workflow every minute (for demonstration)
    # In a real-world scenario, you might use "every 1 day at 02:00" or similar
    time_trigger = TimeTrigger(
        name="Minute Schedule",
        config={
            "schedule_str": "every 1 minute"
        }
    )
    workflow.add_trigger(time_trigger)
    
    # Add actions
    # 1. Extract: Load data from CSV
    extract_action = LoadCSVAction(
        name="Extract Data",
        config={
            "file_path": "data/sales_data.csv",  # Replace with your actual data file
            "parse_dates": ["date"]  # Parse the date column as datetime
        }
    )
    
    # 2. Transform: Clean and aggregate the data
    clean_action = DropNAAction(
        name="Clean Data",
        config={}
    )
    
    fix_types_action = FixDataTypesAction(
        name="Fix Data Types",
        config={
            "column_types": {
                "amount": "float",
                "quantity": "int"
            }
        }
    )
    
    aggregate_action = AggregateAction(
        name="Aggregate Data",
        config={
            "group_by": ["date", "product_category"],
            "aggregations": {
                "amount": "sum",
                "quantity": "sum"
            }
        }
    )
    
    # 3. Load: Save the transformed data
    load_action = SaveCSVAction(
        name="Load Data",
        config={
            "file_path": "data/sales_summary_{now:%Y%m%d_%H%M}.csv",  # Include timestamp in filename
            "index": False
        }
    )
    
    # 4. Notify completion
    notify_action = ConsoleNotifyAction(
        name="Notify ETL Completion",
        config={
            "message": "ETL process completed at {now:%Y-%m-%d %H:%M:%S}",
            "level": "success"
        }
    )
    
    # Add actions to the workflow
    workflow.add_action(extract_action)
    workflow.add_action(clean_action)
    workflow.add_action(fix_types_action)
    workflow.add_action(aggregate_action)
    workflow.add_action(load_action)
    workflow.add_action(notify_action)
    
    # Set up dependencies
    workflow.add_dependency(clean_action, extract_action)
    workflow.add_dependency(fix_types_action, clean_action)
    workflow.add_dependency(aggregate_action, fix_types_action)
    workflow.add_dependency(load_action, aggregate_action)
    workflow.add_dependency(notify_action, load_action)
    
    # Create a workflow runner
    runner = WorkflowRunner()
    
    # Register and start the workflow
    runner.register_workflow(workflow)
    runner.start_workflow(workflow.id)
    
    print("Scheduled ETL workflow started.")
    print("The workflow will run every minute.")
    print("Press Ctrl+C to stop...")
    
    try:
        # Keep the script running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        # Stop the workflow when Ctrl+C is pressed
        runner.stop_workflow(workflow.id)
        print("Scheduled ETL workflow stopped.")

if __name__ == "__main__":
    main()
