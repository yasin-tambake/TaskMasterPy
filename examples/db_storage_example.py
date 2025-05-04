"""
Example demonstrating the database storage for workflows.

This example shows how to:
1. Create a workflow
2. Save it to the database
3. Load it from the database
4. Run it
"""
import os
import pandas as pd
from taskmaster.core.workflow import Workflow
from taskmaster.core.runner import WorkflowRunner
from taskmaster.actions.load_data import LoadCSVAction
from taskmaster.actions.clean_data import DropNAAction
from taskmaster.actions.save_data import SaveCSVAction
from taskmaster.storage.db_storage import WorkflowStorage


def create_sample_data():
    """Create a sample CSV file for testing."""
    # Create a sample DataFrame
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', None, 'Eve'],
        'value': [10.5, 20.3, None, 40.1, 50.9]
    }
    df = pd.DataFrame(data)
    
    # Create the data directory if it doesn't exist
    os.makedirs('./data', exist_ok=True)
    
    # Save the DataFrame to a CSV file
    df.to_csv('./data/sample.csv', index=False)
    print("Created sample data file: ./data/sample.csv")


def create_workflow():
    """Create a simple workflow for testing."""
    # Create a workflow
    workflow = Workflow(
        name="Sample DB Workflow",
        description="A simple workflow to demonstrate database storage"
    )
    
    # Add actions
    load_action = LoadCSVAction(
        name="Load Data",
        config={"file_path": "./data/sample.csv"}
    )
    
    clean_action = DropNAAction(
        name="Clean Data",
        config={}  # Use default settings
    )
    
    save_action = SaveCSVAction(
        name="Save Data",
        config={
            "file_path": "./data/sample_cleaned.csv",
            "index": False
        }
    )
    
    # Add actions to the workflow
    workflow.add_action(load_action)
    workflow.add_action(clean_action)
    workflow.add_action(save_action)
    
    # Add dependencies
    workflow.add_dependency(clean_action, load_action)
    workflow.add_dependency(save_action, clean_action)
    
    return workflow


def main():
    """Run the example."""
    # Create sample data
    create_sample_data()
    
    # Create a workflow
    workflow = create_workflow()
    print(f"Created workflow: {workflow.name} (ID: {workflow.id})")
    
    # Initialize the workflow storage
    storage = WorkflowStorage()
    
    # Save the workflow to the database
    config = {
        "id": workflow.id,
        "name": workflow.name,
        "description": workflow.description,
        "actions": [
            {
                "id": "load_data",
                "type": "load_csv",
                "name": "Load Data",
                "config": {"file_path": "./data/sample.csv"}
            },
            {
                "id": "clean_data",
                "type": "drop_na",
                "name": "Clean Data",
                "config": {},
                "depends_on": ["load_data"]
            },
            {
                "id": "save_data",
                "type": "save_csv",
                "name": "Save Data",
                "config": {
                    "file_path": "./data/sample_cleaned.csv",
                    "index": False
                },
                "depends_on": ["clean_data"]
            }
        ]
    }
    
    storage.save_workflow(workflow.id, config)
    print(f"Saved workflow to database: {workflow.id}")
    
    # List workflows in the database
    workflows = storage.list_workflows()
    print(f"Found {len(workflows)} workflows in the database:")
    for wf in workflows:
        print(f"  - {wf['name']} (ID: {wf['id']})")
    
    # Load the workflow from the database
    loaded_workflow = storage.get_workflow_instance(workflow.id)
    print(f"Loaded workflow from database: {loaded_workflow.name} (ID: {loaded_workflow.id})")
    
    # Run the workflow
    runner = WorkflowRunner()
    runner.register_workflow(loaded_workflow)
    print(f"Running workflow: {loaded_workflow.name}")
    
    # Run the workflow
    context = runner.run_workflow_now(loaded_workflow.id)
    
    print("Workflow completed successfully!")
    print(f"Output file created: ./data/sample_cleaned.csv")
    
    # Export the workflow to a file
    storage.export_to_file(workflow.id, "./examples/exported_workflow.yaml")
    print(f"Exported workflow to file: ./examples/exported_workflow.yaml")


if __name__ == "__main__":
    main()
