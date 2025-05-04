"""
API Data Processing Example using TaskMasterPy

This script demonstrates how to use TaskMasterPy to:
1. Fetch data from an API
2. Process and transform the data
3. Save the results to a CSV file

Author: Yasin
"""

import time
import pandas as pd

from taskmaster.core.workflow import Workflow
from taskmaster.core.runner import WorkflowRunner
from taskmaster.triggers.time_trigger import TimeTrigger
from taskmaster.actions.api import CallAPIAction
from taskmaster.actions.transform_data import NormalizeAction
from taskmaster.actions.clean_data import DropNAAction
from taskmaster.actions.save_data import SaveCSVAction, SaveJSONAction
from taskmaster.actions.notify import ConsoleNotifyAction
from taskmaster.actions.base import BaseAction
from typing import Dict, Any

# Custom action to convert API response to DataFrame
class ConvertToDataFrameAction(BaseAction):
    """Custom action to convert API JSON response to a pandas DataFrame."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize the action."""
        super().__init__(name, config)
    
    def execute(self, context: Dict[str, Any] = None) -> pd.DataFrame:
        """Execute the action to convert JSON to DataFrame.
        
        Args:
            context: Execution context with API response
            
        Returns:
            The converted DataFrame
        """
        context = context or {}
        
        # Get the API response from the previous action
        api_action_id = self.config.get("api_action_id")
        if not api_action_id or api_action_id not in context:
            raise ValueError(f"API action result not found in context: {api_action_id}")
        
        api_response = context[api_action_id]
        
        # Get the JSON path to the data array
        json_path = self.config.get("json_path", "")
        
        # Extract the data using the JSON path
        if json_path:
            import jmespath
            data = jmespath.search(json_path, api_response)
        else:
            data = api_response
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        return df

def main():
    # Create a workflow
    workflow = Workflow(name="API Data Processing", description="Fetch and process data from an API")
    
    # Add a time trigger to run once (for demonstration)
    # You could change this to run on a schedule
    time_trigger = TimeTrigger(
        name="Run Once",
        config={
            "schedule_str": "every 1 hour"  # Change as needed
        }
    )
    workflow.add_trigger(time_trigger)
    
    # 1. Call API to fetch data
    # Using a public API for demonstration
    api_action = CallAPIAction(
        name="Fetch API Data",
        config={
            "url": "https://jsonplaceholder.typicode.com/posts",  # Example API
            "method": "GET",
            "headers": {
                "Content-Type": "application/json"
            }
        }
    )
    
    # 2. Convert API response to DataFrame
    convert_action = ConvertToDataFrameAction(
        name="Convert to DataFrame",
        config={
            "api_action_id": api_action.id,  # Reference to the API action
            "json_path": ""  # No path needed for this API as it returns an array directly
        }
    )
    
    # 3. Clean the data
    clean_action = DropNAAction(
        name="Clean Data",
        config={}
    )
    
    # 4. Save as CSV
    save_csv_action = SaveCSVAction(
        name="Save as CSV",
        config={
            "file_path": "data/api_data.csv",
            "index": False
        }
    )
    
    # 5. Save as JSON (alternative format)
    save_json_action = SaveJSONAction(
        name="Save as JSON",
        config={
            "file_path": "data/api_data.json",
            "orient": "records",
            "indent": 2
        }
    )
    
    # 6. Notify completion
    notify_action = ConsoleNotifyAction(
        name="Notify Completion",
        config={
            "message": "API data processing completed successfully!",
            "level": "success"
        }
    )
    
    # Add actions to the workflow
    workflow.add_action(api_action)
    workflow.add_action(convert_action)
    workflow.add_action(clean_action)
    workflow.add_action(save_csv_action)
    workflow.add_action(save_json_action)
    workflow.add_action(notify_action)
    
    # Set up dependencies
    workflow.add_dependency(convert_action, api_action)
    workflow.add_dependency(clean_action, convert_action)
    workflow.add_dependency(save_csv_action, clean_action)
    workflow.add_dependency(save_json_action, clean_action)
    workflow.add_dependency(notify_action, save_csv_action)
    workflow.add_dependency(notify_action, save_json_action)
    
    # Create a workflow runner
    runner = WorkflowRunner()
    
    # Register the workflow
    runner.register_workflow(workflow)
    
    # Run the workflow immediately
    print("Running API data processing workflow...")
    result = runner.run_workflow_now(workflow.id)
    
    print("Workflow execution completed!")

if __name__ == "__main__":
    main()
