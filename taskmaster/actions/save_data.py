"""
Data saving actions for TaskMasterPy.

This module defines actions for saving data to various destinations,
such as CSV files, JSON files, Excel files, or databases.
"""
import os
from typing import Dict, Any, Optional, List, Union
import pandas as pd
import json
import sqlite3

from taskmaster.actions.base import BaseAction


class SaveDataAction(BaseAction):
    """Base class for actions that save data to various destinations."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new save data action.
        
        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
        """
        super().__init__(name, config)
    
    def execute(self, context: Dict[str, Any] = None) -> str:
        """Execute the action to save data.
        
        Args:
            context: Execution context
            
        Returns:
            The path or identifier where the data was saved
        """
        raise NotImplementedError("Subclasses must implement execute()")
    
    def _get_input_dataframe(self, context: Dict[str, Any]) -> pd.DataFrame:
        """Get the input DataFrame from the context.
        
        Args:
            context: Execution context
            
        Returns:
            The input DataFrame
        """
        # Check if there's a specific input key in the config
        input_key = self.config.get("input_key", None)
        
        if input_key and input_key in context:
            # Use the specified input key
            df = context[input_key]
        else:
            # Try to find a DataFrame in the context
            for key, value in context.items():
                if isinstance(value, pd.DataFrame):
                    df = value
                    break
            else:
                raise ValueError("No DataFrame found in context")
        
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"Expected a DataFrame, got {type(df)}")
        
        return df


class SaveCSVAction(SaveDataAction):
    """Action to save data to a CSV file."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new save CSV action.
        
        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
                - file_path: Path to the CSV file
                - index: Whether to include the index (default: False)
                - header: Whether to include the header (default: True)
                - delimiter: Field delimiter (default: ',')
                - encoding: File encoding (default: 'utf-8')
        """
        super().__init__(name, config)
    
    def execute(self, context: Dict[str, Any] = None) -> str:
        """Execute the action to save data to a CSV file.
        
        Args:
            context: Execution context, must contain a DataFrame
            
        Returns:
            The path where the data was saved
        """
        context = context or {}
        
        # Get the input DataFrame
        df = self._get_input_dataframe(context)
        
        # Get parameters from config
        file_path = self.config.get("file_path", "")
        index = self.config.get("index", False)
        header = self.config.get("header", True)
        delimiter = self.config.get("delimiter", ",")
        encoding = self.config.get("encoding", "utf-8")
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
        
        # Save the DataFrame to CSV
        df.to_csv(
            file_path,
            index=index,
            header=header,
            sep=delimiter,
            encoding=encoding
        )
        
        return file_path


class SaveJSONAction(SaveDataAction):
    """Action to save data to a JSON file."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new save JSON action.
        
        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
                - file_path: Path to the JSON file
                - orient: Format of the JSON data (default: 'records')
                - indent: Number of spaces for indentation (default: 2)
                - encoding: File encoding (default: 'utf-8')
        """
        super().__init__(name, config)
    
    def execute(self, context: Dict[str, Any] = None) -> str:
        """Execute the action to save data to a JSON file.
        
        Args:
            context: Execution context, must contain a DataFrame
            
        Returns:
            The path where the data was saved
        """
        context = context or {}
        
        # Get the input DataFrame
        df = self._get_input_dataframe(context)
        
        # Get parameters from config
        file_path = self.config.get("file_path", "")
        orient = self.config.get("orient", "records")
        indent = self.config.get("indent", 2)
        encoding = self.config.get("encoding", "utf-8")
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
        
        # Save the DataFrame to JSON
        df.to_json(
            file_path,
            orient=orient,
            indent=indent,
            force_ascii=False
        )
        
        return file_path


class SaveExcelAction(SaveDataAction):
    """Action to save data to an Excel file."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new save Excel action.
        
        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
                - file_path: Path to the Excel file
                - sheet_name: Name of the sheet (default: 'Sheet1')
                - index: Whether to include the index (default: False)
                - header: Whether to include the header (default: True)
        """
        super().__init__(name, config)
    
    def execute(self, context: Dict[str, Any] = None) -> str:
        """Execute the action to save data to an Excel file.
        
        Args:
            context: Execution context, must contain a DataFrame
            
        Returns:
            The path where the data was saved
        """
        context = context or {}
        
        # Get the input DataFrame
        df = self._get_input_dataframe(context)
        
        # Get parameters from config
        file_path = self.config.get("file_path", "")
        sheet_name = self.config.get("sheet_name", "Sheet1")
        index = self.config.get("index", False)
        header = self.config.get("header", True)
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
        
        # Save the DataFrame to Excel
        df.to_excel(
            file_path,
            sheet_name=sheet_name,
            index=index,
            header=header
        )
        
        return file_path


class SaveSQLAction(SaveDataAction):
    """Action to save data to a SQL database."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new save SQL action.
        
        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
                - connection_string: Database connection string
                - table_name: Name of the table
                - if_exists: What to do if the table exists (default: 'replace')
                  Options: 'fail', 'replace', 'append'
                - index: Whether to include the index (default: False)
        """
        super().__init__(name, config)
    
    def execute(self, context: Dict[str, Any] = None) -> str:
        """Execute the action to save data to a SQL database.
        
        Args:
            context: Execution context, must contain a DataFrame
            
        Returns:
            The table name where the data was saved
        """
        context = context or {}
        
        # Get the input DataFrame
        df = self._get_input_dataframe(context)
        
        # Get parameters from config
        connection_string = self.config.get("connection_string", "")
        table_name = self.config.get("table_name", "")
        if_exists = self.config.get("if_exists", "replace")
        index = self.config.get("index", False)
        
        # For simplicity, we'll only support SQLite for now
        # A more complete implementation would support multiple database types
        
        # Connect to the database
        conn = sqlite3.connect(connection_string)
        
        # Save the DataFrame to the database
        df.to_sql(
            table_name,
            conn,
            if_exists=if_exists,
            index=index
        )
        
        # Close the connection
        conn.close()
        
        return table_name
