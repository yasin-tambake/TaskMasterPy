"""
Autopilot Example using TaskMasterPy

This script demonstrates how to use TaskMasterPy's autopilot feature
for quick and simple data processing tasks.

Author: Kaushik
"""

# Import TaskMasterPy
import taskmaster as tm
from taskmaster.core.workflow import Workflow
from taskmaster.core.runner import WorkflowRunner
from taskmaster.actions.load_data import LoadCSVAction
from taskmaster.actions.clean_data import DropNAAction
from taskmaster.actions.transform_data import NormalizeAction
from taskmaster.actions.save_data import SaveCSVAction

def main():
    print("Example 1: Basic CSV processing")
    # Create a workflow for basic processing
    workflow = Workflow(name="Basic Processing")

    # Add actions
    load_action = LoadCSVAction(
        name="Load CSV Data",
        config={"file_path": "data/sample.csv"}
    )

    save_action = SaveCSVAction(
        name="Save Data",
        config={
            "file_path": "data/processed_sample.csv",
            "index": False
        }
    )

    # Add actions to the workflow
    workflow.add_action(load_action)
    workflow.add_action(save_action)
    workflow.add_dependency(save_action, load_action)

    # Run the workflow
    runner = WorkflowRunner()
    runner.register_workflow(workflow)
    runner.run_workflow_now(workflow.id)
    print("Basic processing completed!")

    print("\nExample 2: CSV processing with normalization")
    # Create a workflow for normalized processing
    workflow2 = Workflow(name="Normalized Processing")

    # Add actions
    load_action2 = LoadCSVAction(
        name="Load CSV Data",
        config={"file_path": "data/sample.csv"}
    )

    normalize_action = NormalizeAction(
        name="Normalize Data",
        config={"method": "minmax"}
    )

    save_action2 = SaveCSVAction(
        name="Save Normalized Data",
        config={
            "file_path": "data/normalized_sample.csv",
            "index": False
        }
    )

    # Add actions to the workflow
    workflow2.add_action(load_action2)
    workflow2.add_action(normalize_action)
    workflow2.add_action(save_action2)
    workflow2.add_dependency(normalize_action, load_action2)
    workflow2.add_dependency(save_action2, normalize_action)

    # Run the workflow
    runner2 = WorkflowRunner()
    runner2.register_workflow(workflow2)
    runner2.run_workflow_now(workflow2.id)
    print("Normalized processing completed!")

    print("\nExample 3: CSV processing with multiple transformations")
    # Create a workflow for full processing
    workflow3 = Workflow(name="Full Processing")

    # Add actions
    load_action3 = LoadCSVAction(
        name="Load CSV Data",
        config={"file_path": "data/sample.csv"}
    )

    clean_action = DropNAAction(
        name="Clean Data",
        config={}
    )

    normalize_action2 = NormalizeAction(
        name="Normalize Data",
        config={"method": "minmax"}
    )

    save_action3 = SaveCSVAction(
        name="Save Fully Processed Data",
        config={
            "file_path": "data/fully_processed_sample.csv",
            "index": False
        }
    )

    # Add actions to the workflow
    workflow3.add_action(load_action3)
    workflow3.add_action(clean_action)
    workflow3.add_action(normalize_action2)
    workflow3.add_action(save_action3)
    workflow3.add_dependency(clean_action, load_action3)
    workflow3.add_dependency(normalize_action2, clean_action)
    workflow3.add_dependency(save_action3, normalize_action2)

    # Run the workflow
    runner3 = WorkflowRunner()
    runner3.register_workflow(workflow3)
    runner3.run_workflow_now(workflow3.id)
    print("Full processing completed!")

    print("\nAll examples completed successfully!")

if __name__ == "__main__":
    main()
