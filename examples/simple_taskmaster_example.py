"""
Simple TaskMasterPy Example

This script demonstrates how to use TaskMasterPy with a simplified import approach.

Author: Yasin
"""

# Import TaskMasterPy with an alias
import taskmaster as tm

def main():
    # Create a workflow
    workflow = tm.core.workflow.Workflow(name="Simple Workflow", description="A simple example workflow")
    
    # Add actions
    # 1. Load CSV data
    load_action = tm.actions.load_data.LoadCSVAction(
        name="Load CSV Data", 
        config={
            "file_path": "data/sample.csv",  # Replace with your actual CSV file path
            "encoding": "utf-8"
        }
    )
    
    # 2. Clean data by removing NA values
    clean_action = tm.actions.clean_data.DropNAAction(
        name="Remove NA Values",
        config={}  # Default configuration
    )
    
    # 3. Save the cleaned data
    save_action = tm.actions.save_data.SaveCSVAction(
        name="Save Cleaned Data",
        config={
            "file_path": "data/cleaned_sample.csv",  # Output file path
            "index": False
        }
    )
    
    # Add actions to the workflow
    workflow.add_action(load_action)
    workflow.add_action(clean_action)
    workflow.add_action(save_action)
    
    # Set up dependencies (execution order)
    workflow.add_dependency(clean_action, load_action)
    workflow.add_dependency(save_action, clean_action)
    
    # Create a workflow runner
    runner = tm.core.runner.WorkflowRunner()
    
    # Register the workflow
    runner.register_workflow(workflow)
    
    # Run the workflow
    print("Running workflow...")
    result = runner.run_workflow_now(workflow.id)
    
    print("Workflow execution completed!")
    
    # Alternatively, use the autopilot function for a one-liner
    # tm.autopilot(data_path="data/sample.csv", output_path="data/processed_sample.csv", normalize=True)

if __name__ == "__main__":
    main()
