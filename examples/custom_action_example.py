"""
Custom Action Example using TaskMasterPy

This script demonstrates how to create and use custom actions with TaskMasterPy.

Author: Yasin
"""

import taskmaster as tm
import pandas as pd
import logging
from typing import Dict, Any

# Define a custom action by inheriting from BaseAction
class CustomScalingAction(tm.actions.base.BaseAction):
    """Custom action to scale numeric columns by a specified factor."""

    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize the custom scaling action.

        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
                - columns: List of columns to scale (if None, scales all numeric columns)
                - scale_factor: Factor to multiply values by (default: 2.0)
        """
        super().__init__(name, config)
        self.logger = logging.getLogger(f"taskmaster.action.{self.__class__.__name__}")

    def execute(self, context: Dict[str, Any] = None) -> pd.DataFrame:
        """Execute the action to scale numeric columns.

        Args:
            context: Execution context

        Returns:
            The DataFrame with scaled columns
        """
        context = context or {}

        # In a real implementation, we would get the input from the previous action
        # For simplicity in this example, we'll just load the data directly
        df = pd.read_csv("data/sample.csv")

        # Get parameters from config
        columns = self.config.get("columns", None)
        scale_factor = self.config.get("scale_factor", 2.0)

        # If no columns specified, find all numeric columns
        if columns is None:
            columns = df.select_dtypes(include=['number']).columns.tolist()

        # Scale the specified columns
        for col in columns:
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col] * scale_factor
            else:
                self.logger.warning(f"Column '{col}' not found or not numeric, skipping")

        return df

def main():
    # Create a workflow
    workflow = tm.core.workflow.Workflow(name="Custom Action Workflow")

    # Add actions
    # 1. Load CSV data
    load_action = tm.actions.load_data.LoadCSVAction(
        name="Load CSV Data",
        config={
            "file_path": "data/sample.csv"
        }
    )

    # 2. Apply our custom scaling action
    scale_action = CustomScalingAction(
        name="Scale Numeric Columns",
        config={
            "columns": ["price", "quantity"],  # Specify columns to scale
            "scale_factor": 1.5  # Scale by 1.5x
        }
    )

    # 3. Save the processed data
    save_action = tm.actions.save_data.SaveCSVAction(
        name="Save Scaled Data",
        config={
            "file_path": "data/scaled_sample.csv",
            "index": False
        }
    )

    # Add actions to the workflow
    workflow.add_action(load_action)
    workflow.add_action(scale_action)
    workflow.add_action(save_action)

    # Set up dependencies
    workflow.add_dependency(scale_action, load_action)
    workflow.add_dependency(save_action, scale_action)

    # Create a workflow runner
    runner = tm.core.runner.WorkflowRunner()

    # Register and run the workflow
    runner.register_workflow(workflow)
    print("Running workflow with custom action...")
    result = runner.run_workflow_now(workflow.id)

    print("Workflow execution completed!")

if __name__ == "__main__":
    main()
