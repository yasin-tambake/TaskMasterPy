"""
File Monitoring Example using TaskMasterPy

This script demonstrates how to use TaskMasterPy to:
1. Monitor a directory for new or modified CSV files
2. Process each file when changes are detected
3. Save the processed data to a new location

Author: Adriella
"""

import time
import os

from taskmaster.core.workflow import Workflow
from taskmaster.core.runner import WorkflowRunner
from taskmaster.triggers.file_trigger import FileTrigger
from taskmaster.actions.load_data import LoadCSVAction
from taskmaster.actions.clean_data import DropNAAction
from taskmaster.actions.transform_data import NormalizeAction
from taskmaster.actions.save_data import SaveCSVAction
from taskmaster.actions.notify import ConsoleNotifyAction

def main():
    # Create a workflow
    workflow = Workflow(name="File Monitor Workflow", description="Monitor and process CSV files")
    
    # Add a file trigger to watch for CSV files
    file_trigger = FileTrigger(
        name="CSV File Monitor",
        config={
            "path": "data/input",  # Directory to monitor
            "patterns": ["*.csv"],  # Only watch for CSV files
            "events": ["created", "modified"],  # Trigger on file creation or modification
            "recursive": False  # Don't monitor subdirectories
        }
    )
    workflow.add_trigger(file_trigger)
    
    # Add actions
    # 1. Load the CSV file (path will be provided by the trigger)
    load_action = LoadCSVAction(
        name="Load CSV File",
        config={
            "file_path": "{event_data.path}"  # Use the path from the trigger event
        }
    )
    
    # 2. Clean the data
    clean_action = DropNAAction(
        name="Clean Data",
        config={}
    )
    
    # 3. Normalize the data
    normalize_action = NormalizeAction(
        name="Normalize Data",
        config={
            "method": "minmax"  # Use min-max normalization
        }
    )
    
    # 4. Save the processed data
    save_action = SaveCSVAction(
        name="Save Processed Data",
        config={
            # Extract the filename from the path and save to the output directory
            "file_path": "data/output/processed_{event_data.path|basename}",
            "index": False
        }
    )
    
    # 5. Add a notification
    notify_action = ConsoleNotifyAction(
        name="Notify Processing",
        config={
            "message": "Processed file: {event_data.path}",
            "level": "info"
        }
    )
    
    # Add actions to the workflow
    workflow.add_action(load_action)
    workflow.add_action(clean_action)
    workflow.add_action(normalize_action)
    workflow.add_action(save_action)
    workflow.add_action(notify_action)
    
    # Set up dependencies
    workflow.add_dependency(clean_action, load_action)
    workflow.add_dependency(normalize_action, clean_action)
    workflow.add_dependency(save_action, normalize_action)
    workflow.add_dependency(notify_action, save_action)
    
    # Create a workflow runner
    runner = WorkflowRunner()
    
    # Register and start the workflow
    runner.register_workflow(workflow)
    runner.start_workflow(workflow.id)
    
    print(f"Monitoring directory: data/input")
    print("Press Ctrl+C to stop...")
    
    # Create the input and output directories if they don't exist
    os.makedirs("data/input", exist_ok=True)
    os.makedirs("data/output", exist_ok=True)
    
    try:
        # Keep the script running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        # Stop the workflow when Ctrl+C is pressed
        runner.stop_workflow(workflow.id)
        print("Monitoring stopped.")

if __name__ == "__main__":
    main()
