"""
YAML Configuration Example using TaskMasterPy

This script demonstrates how to use TaskMasterPy with YAML configuration files
to define and run workflows.

Author: Shatrugna
"""

import taskmaster as tm
import os
import yaml

def main():
    # Define a sample workflow configuration
    workflow_config = {
        "name": "YAML Config Workflow",
        "description": "A workflow defined in YAML",
        
        "triggers": [
            {
                "type": "time",
                "name": "Manual Trigger",
                "config": {
                    "schedule_str": "every 1 hour"
                }
            }
        ],
        
        "actions": [
            {
                "id": "load_data",
                "type": "load_csv",
                "name": "Load CSV Data",
                "config": {
                    "file_path": "data/test.csv"
                }
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
                "name": "Save Cleaned Data",
                "config": {
                    "file_path": "data/test_cleaned.csv",
                    "index": False
                },
                "depends_on": ["clean_data"]
            },
            {
                "id": "notify",
                "type": "console_notify",
                "name": "Notify Completion",
                "config": {
                    "message": "YAML workflow completed successfully",
                    "level": "success"
                },
                "depends_on": ["save_data"]
            }
        ]
    }
    
    # Create the data directory if it doesn't exist
    os.makedirs("data", exist_ok=True)
    
    # Save the configuration to a YAML file
    yaml_path = "data/workflow_config.yaml"
    with open(yaml_path, "w") as f:
        yaml.dump(workflow_config, f, default_flow_style=False)
    
    print(f"Saved workflow configuration to {yaml_path}")
    
    # Method 1: Load the workflow from the YAML file
    print("\nMethod 1: Loading workflow from YAML file")
    with open(yaml_path, "r") as f:
        loaded_config = yaml.safe_load(f)
    
    # Create a workflow from the loaded configuration
    workflow = tm.utils.config.load_workflow_from_config(loaded_config)
    
    # Create a workflow runner
    runner = tm.core.runner.WorkflowRunner()
    
    # Register and run the workflow
    runner.register_workflow(workflow)
    print("Running workflow from YAML configuration...")
    result = runner.run_workflow_now(workflow.id)
    
    print("Workflow execution completed!")
    
    # Method 2: Using autopilot with a config file
    print("\nMethod 2: Using autopilot with a config file")
    print("This would run the workflow defined in the YAML file:")
    print("tm.autopilot(config_path=yaml_path)")
    
    # Note: Uncomment the line below to actually run it
    # result = tm.autopilot(config_path=yaml_path)
    # print("Autopilot execution completed!")

if __name__ == "__main__":
    main()
