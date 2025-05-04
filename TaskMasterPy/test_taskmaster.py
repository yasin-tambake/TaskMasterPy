"""
Simple test script for TaskMasterPy.

This script creates a simple workflow and runs it to verify that
TaskMasterPy is working correctly.
"""
import os
import pandas as pd
from taskmaster.core.workflow import Workflow
from taskmaster.core.runner import WorkflowRunner
from taskmaster.actions.load_data import LoadCSVAction
from taskmaster.actions.clean_data import DropNAAction
from taskmaster.actions.save_data import SaveCSVAction
from taskmaster.actions.notify import ConsoleNotifyAction


def create_test_data():
    """Create a test CSV file."""
    # Create a sample DataFrame
    data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", None],
        "age": [25, 30, None, 40, 45],
        "score": [85.5, 90.0, 78.5, None, 92.5]
    }
    df = pd.DataFrame(data)
    
    # Create the data directory if it doesn't exist
    os.makedirs("./data", exist_ok=True)
    
    # Save the DataFrame to a CSV file
    df.to_csv("./data/test.csv", index=False)
    
    print(f"Created test data file: ./data/test.csv")
    return "./data/test.csv"


def test_workflow():
    """Create and run a simple workflow."""
    # Create test data
    test_file = create_test_data()
    
    # Create the workflow
    workflow = Workflow(name="Test Workflow")
    
    # Create actions
    load_action = LoadCSVAction(
        name="Load Test Data",
        config={"file_path": test_file}
    )
    
    clean_action = DropNAAction(
        name="Clean Data",
        config={}
    )
    
    save_action = SaveCSVAction(
        name="Save Cleaned Data",
        config={
            "file_path": "./data/test_cleaned.csv",
            "index": False
        }
    )
    
    notify_action = ConsoleNotifyAction(
        name="Notify Completion",
        config={
            "message": "Test workflow completed successfully",
            "level": "success"
        }
    )
    
    # Add actions to the workflow
    workflow.add_action(load_action)
    workflow.add_action(clean_action)
    workflow.add_action(save_action)
    workflow.add_action(notify_action)
    
    # Add dependencies
    workflow.add_dependency(clean_action, load_action)
    workflow.add_dependency(save_action, clean_action)
    workflow.add_dependency(notify_action, save_action)
    
    # Create a runner
    runner = WorkflowRunner()
    runner.register_workflow(workflow)
    
    # Run the workflow
    print("Running test workflow...")
    context = runner.run_workflow_now(workflow.id)
    
    # Check the results
    if os.path.exists("./data/test_cleaned.csv"):
        print("Test passed: Cleaned data file was created")
        
        # Load the cleaned data to verify
        cleaned_df = pd.read_csv("./data/test_cleaned.csv")
        print(f"Cleaned data has {len(cleaned_df)} rows (should be less than 5)")
        
        return True
    else:
        print("Test failed: Cleaned data file was not created")
        return False


if __name__ == "__main__":
    test_workflow()
