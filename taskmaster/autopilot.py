"""
Autopilot mode for TaskMasterPy.

This module provides a simplified interface for running workflows.
"""
import os
import pandas as pd
from typing import Dict, Any, Optional, List, Union

from taskmaster.core.workflow import Workflow
from taskmaster.core.runner import WorkflowRunner
from taskmaster.utils.config import load_workflow_from_config, load_workflow_config
from taskmaster.actions.load_data import LoadCSVAction, LoadJSONAction, LoadExcelAction
from taskmaster.actions.clean_data import DropNAAction, FixDataTypesAction
from taskmaster.actions.transform_data import NormalizeAction
from taskmaster.actions.save_data import SaveCSVAction, SaveJSONAction


def autopilot(config_path: Optional[str] = None, data_path: Optional[str] = None, **kwargs) -> Dict[str, Any]:
    """Run a workflow in autopilot mode.
    
    This function provides a simplified interface for running workflows.
    If a config_path is provided, it loads and runs the workflow from the config file.
    If a data_path is provided, it creates a simple workflow to load, clean, and save the data.
    
    Args:
        config_path: Path to a workflow configuration file
        data_path: Path to a data file (CSV, JSON, Excel)
        **kwargs: Additional parameters for the workflow
            - output_path: Path to save the processed data
            - clean: Whether to clean the data (default: True)
            - normalize: Whether to normalize numeric columns (default: False)
            - output_format: Format to save the data (default: inferred from output_path)
            - infer_types: Whether to infer data types (default: True)
            - drop_na: Whether to drop NA values (default: True)
    
    Returns:
        The workflow context after execution
    """
    if config_path:
        # Load and run the workflow from the config file
        config = load_workflow_config(config_path)
        workflow = load_workflow_from_config(config)
        
        runner = WorkflowRunner()
        runner.register_workflow(workflow)
        
        return runner.run_workflow_now(workflow.id, kwargs)
    
    elif data_path:
        # Create a simple workflow to load, clean, and save the data
        workflow = create_data_workflow(data_path, **kwargs)
        
        runner = WorkflowRunner()
        runner.register_workflow(workflow)
        
        return runner.run_workflow_now(workflow.id)
    
    else:
        raise ValueError("Either config_path or data_path must be provided")


def create_data_workflow(data_path: str, **kwargs) -> Workflow:
    """Create a simple workflow to load, clean, and save data.
    
    Args:
        data_path: Path to a data file (CSV, JSON, Excel)
        **kwargs: Additional parameters for the workflow
            - output_path: Path to save the processed data
            - clean: Whether to clean the data (default: True)
            - normalize: Whether to normalize numeric columns (default: False)
            - output_format: Format to save the data (default: inferred from output_path)
            - infer_types: Whether to infer data types (default: True)
            - drop_na: Whether to drop NA values (default: True)
    
    Returns:
        The created workflow
    """
    # Get parameters from kwargs
    output_path = kwargs.get("output_path", None)
    clean = kwargs.get("clean", True)
    normalize = kwargs.get("normalize", False)
    output_format = kwargs.get("output_format", None)
    infer_types = kwargs.get("infer_types", True)
    drop_na = kwargs.get("drop_na", True)
    
    # Create the workflow
    workflow = Workflow(name=f"Autopilot_{os.path.basename(data_path)}")
    
    # Create the load action based on the file extension
    load_action = None
    if data_path.endswith((".csv", ".txt")):
        load_action = LoadCSVAction(name="Load Data", config={"file_path": data_path})
    elif data_path.endswith((".json")):
        load_action = LoadJSONAction(name="Load Data", config={"file_path": data_path})
    elif data_path.endswith((".xlsx", ".xls")):
        load_action = LoadExcelAction(name="Load Data", config={"file_path": data_path})
    else:
        raise ValueError(f"Unsupported file format: {data_path}")
    
    workflow.add_action(load_action)
    
    # Create cleaning actions if requested
    if clean:
        # Create a fix data types action
        fix_types_action = FixDataTypesAction(
            name="Fix Data Types",
            config={"infer_types": infer_types}
        )
        workflow.add_action(fix_types_action)
        workflow.add_dependency(fix_types_action, load_action)
        
        # Create a drop NA action if requested
        if drop_na:
            drop_na_action = DropNAAction(
                name="Drop NA Values",
                config={}
            )
            workflow.add_action(drop_na_action)
            workflow.add_dependency(drop_na_action, fix_types_action)
        
        # Create a normalize action if requested
        if normalize:
            normalize_action = NormalizeAction(
                name="Normalize Data",
                config={"method": "minmax"}
            )
            workflow.add_action(normalize_action)
            
            if drop_na:
                workflow.add_dependency(normalize_action, drop_na_action)
            else:
                workflow.add_dependency(normalize_action, fix_types_action)
    
    # Create a save action if output_path is provided
    if output_path:
        # Determine the output format
        if output_format:
            save_format = output_format
        else:
            if output_path.endswith((".csv", ".txt")):
                save_format = "csv"
            elif output_path.endswith((".json")):
                save_format = "json"
            elif output_path.endswith((".xlsx", ".xls")):
                save_format = "excel"
            else:
                save_format = "csv"
        
        # Create the save action
        save_action = None
        if save_format == "csv":
            save_action = SaveCSVAction(
                name="Save Data",
                config={"file_path": output_path}
            )
        elif save_format == "json":
            save_action = SaveJSONAction(
                name="Save Data",
                config={"file_path": output_path}
            )
        
        if save_action:
            workflow.add_action(save_action)
            
            # Add dependency to the last action in the chain
            if normalize:
                workflow.add_dependency(save_action, normalize_action)
            elif drop_na:
                workflow.add_dependency(save_action, drop_na_action)
            elif clean:
                workflow.add_dependency(save_action, fix_types_action)
            else:
                workflow.add_dependency(save_action, load_action)
    
    return workflow


def process_dataframe(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """Process a DataFrame using TaskMasterPy.
    
    This function provides a simplified interface for processing a DataFrame.
    
    Args:
        df: The DataFrame to process
        **kwargs: Additional parameters for processing
            - clean: Whether to clean the data (default: True)
            - normalize: Whether to normalize numeric columns (default: False)
            - infer_types: Whether to infer data types (default: True)
            - drop_na: Whether to drop NA values (default: True)
    
    Returns:
        The processed DataFrame
    """
    # Get parameters from kwargs
    clean = kwargs.get("clean", True)
    normalize = kwargs.get("normalize", False)
    infer_types = kwargs.get("infer_types", True)
    drop_na = kwargs.get("drop_na", True)
    
    # Process the DataFrame
    result = df.copy()
    
    if clean:
        # Fix data types if requested
        if infer_types:
            for column in result.columns:
                # Try to convert to numeric
                try:
                    result[column] = pd.to_numeric(result[column])
                except:
                    # Try to convert to datetime
                    try:
                        result[column] = pd.to_datetime(result[column])
                    except:
                        # Leave as is
                        pass
        
        # Drop NA values if requested
        if drop_na:
            result = result.dropna()
        
        # Normalize numeric columns if requested
        if normalize:
            for column in result.select_dtypes(include=["number"]).columns:
                min_val = result[column].min()
                max_val = result[column].max()
                if max_val > min_val:
                    result[column] = (result[column] - min_val) / (max_val - min_val)
    
    return result
