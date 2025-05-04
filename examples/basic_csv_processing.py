"""
Basic CSV Processing Example using TaskMasterPy

This script demonstrates how to use TaskMasterPy to:
1. Load data from a CSV file
2. Clean the data by removing NA values
3. Save the cleaned data to a new CSV file

Author: Yasin
"""

# Import the TaskMasterPy library
from taskmaster import autopilot
from taskmaster.core.workflow import Workflow
from taskmaster.core.runner import WorkflowRunner
from taskmaster.actions.load_data import LoadCSVAction
from taskmaster.actions.clean_data import DropNAAction, FixDataTypesAction
from taskmaster.actions.save_data import SaveCSVAction
from taskmaster.actions.notify import ConsoleNotifyAction

def main():
    # Method 1: Using autopilot for simple processing
    print("Method 1: Using autopilot")
    # Uncomment the line below and replace with your actual CSV file path
    # autopilot(data_path="data/sample.csv", output_path="data/processed_sample.csv", normalize=True)
    
    # Method 2: Creating a custom workflow
    print("Method 2: Creating a custom workflow")
    
    # Create a workflow
    workflow = Workflow(name="CSV Processing Workflow", description="Load, clean, and save CSV data")
    
    # Add actions
    # 1. Load CSV data
    load_action = LoadCSVAction(
        name="Load CSV Data", 
        config={
            "file_path": "data/sample.csv",  # Replace with your actual CSV file path
            "encoding": "utf-8"
        }
    )
    
    # 2. Clean data by removing NA values
    clean_action = DropNAAction(
        name="Remove NA Values",
        config={}  # Default configuration
    )
    
    # 3. Fix data types
    fix_types_action = FixDataTypesAction(
        name="Fix Data Types",
        config={"infer_types": True}
    )
    
    # 4. Save the cleaned data
    save_action = SaveCSVAction(
        name="Save Cleaned Data",
        config={
            "file_path": "data/cleaned_sample.csv",  # Output file path
            "index": False,
            "encoding": "utf-8"
        }
    )
    
    # 5. Add a notification action
    notify_action = ConsoleNotifyAction(
        name="Notify Completion",
        config={
            "message": "CSV processing completed successfully!",
            "level": "success"
        }
    )
    
    # Add actions to the workflow
    workflow.add_action(load_action)
    workflow.add_action(clean_action)
    workflow.add_action(fix_types_action)
    workflow.add_action(save_action)
    workflow.add_action(notify_action)
    
    # Set up dependencies (execution order)
    workflow.add_dependency(clean_action, load_action)  # clean_action depends on load_action
    workflow.add_dependency(fix_types_action, clean_action)  # fix_types_action depends on clean_action
    workflow.add_dependency(save_action, fix_types_action)  # save_action depends on fix_types_action
    workflow.add_dependency(notify_action, save_action)  # notify_action depends on save_action
    
    # Create a workflow runner
    runner = WorkflowRunner()
    
    # Register the workflow
    runner.register_workflow(workflow)
    
    # Run the workflow
    print("Running workflow...")
    result = runner.run_workflow_now(workflow.id)
    
    print("Workflow execution completed!")
    
if __name__ == "__main__":
    main()
