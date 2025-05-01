"""
Data loading actions for TaskMasterPy.

This module defines actions for loading data from various sources,
such as CSV files, JSON files, Excel files, or databases.
"""
import os
from typing import Dict, Any, Optional, List, Union
import pandas as pd
import json
import sqlite3

from taskmaster.actions.base import BaseAction


class LoadDataAction(BaseAction):
    """Base class for actions that load data from various sources."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new load data action.
        
        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
        """
        super().__init__(name, config)
        self.data = None
    
    def execute(self, context: Dict[str, Any] = None) -> pd.DataFrame:
        """Execute the action to load data.
        
        Args:
            context: Execution context
            
        Returns:
            The loaded data as a pandas DataFrame
        """
        raise NotImplementedError("Subclasses must implement execute()")


class LoadCSVAction(LoadDataAction):
    """Action to load data from a CSV file."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new load CSV action.
        
        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
                - file_path: Path to the CSV file
                - delimiter: Field delimiter (default: ',')
                - header: Row to use as column names (default: 0)
                - encoding: File encoding (default: 'utf-8')
                - skip_rows: Number of rows to skip (default: 0)
                - parse_dates: Columns to parse as dates
        """
        super().__init__(name, config)
    
    def execute(self, context: Dict[str, Any] = None) -> pd.DataFrame:
        """Execute the action to load data from a CSV file.
        
        Args:
            context: Execution context
            
        Returns:
            The loaded data as a pandas DataFrame
        """
        context = context or {}
        
        # Get parameters from config
        file_path = self.config.get("file_path", "")
        delimiter = self.config.get("delimiter", ",")
        header = self.config.get("header", 0)
        encoding = self.config.get("encoding", "utf-8")
        skip_rows = self.config.get("skip_rows", 0)
        parse_dates = self.config.get("parse_dates", False)
        
        # Check if file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        # Load the CSV file
        self.data = pd.read_csv(
            file_path,
            delimiter=delimiter,
            header=header,
            encoding=encoding,
            skiprows=skip_rows,
            parse_dates=parse_dates
        )
        
        return self.data


class LoadJSONAction(LoadDataAction):
    """Action to load data from a JSON file."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new load JSON action.
        
        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
                - file_path: Path to the JSON file
                - orient: Format of the JSON data (default: 'records')
                - encoding: File encoding (default: 'utf-8')
        """
        super().__init__(name, config)
    
    def execute(self, context: Dict[str, Any] = None) -> pd.DataFrame:
        """Execute the action to load data from a JSON file.
        
        Args:
            context: Execution context
            
        Returns:
            The loaded data as a pandas DataFrame
        """
        context = context or {}
        
        # Get parameters from config
        file_path = self.config.get("file_path", "")
        orient = self.config.get("orient", "records")
        encoding = self.config.get("encoding", "utf-8")
        
        # Check if file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"JSON file not found: {file_path}")
        
        # Load the JSON file
        with open(file_path, "r", encoding=encoding) as f:
            json_data = json.load(f)
        
        # Convert to DataFrame
        self.data = pd.json_normalize(json_data) if orient == "records" else pd.DataFrame(json_data)
        
        return self.data


class LoadExcelAction(LoadDataAction):
    """Action to load data from an Excel file."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new load Excel action.
        
        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
                - file_path: Path to the Excel file
                - sheet_name: Name or index of the sheet to load (default: 0)
                - header: Row to use as column names (default: 0)
                - skip_rows: Number of rows to skip (default: 0)
                - parse_dates: Columns to parse as dates
        """
        super().__init__(name, config)
    
    def execute(self, context: Dict[str, Any] = None) -> pd.DataFrame:
        """Execute the action to load data from an Excel file.
        
        Args:
            context: Execution context
            
        Returns:
            The loaded data as a pandas DataFrame
        """
        context = context or {}
        
        # Get parameters from config
        file_path = self.config.get("file_path", "")
        sheet_name = self.config.get("sheet_name", 0)
        header = self.config.get("header", 0)
        skip_rows = self.config.get("skip_rows", 0)
        parse_dates = self.config.get("parse_dates", False)
        
        # Check if file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Excel file not found: {file_path}")
        
        # Load the Excel file
        self.data = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            header=header,
            skiprows=skip_rows,
            parse_dates=parse_dates
        )
        
        return self.data


class LoadSQLAction(LoadDataAction):
    """Action to load data from a SQL database."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new load SQL action.
        
        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
                - connection_string: Database connection string
                - query: SQL query to execute
                - params: Parameters for the SQL query
        """
        super().__init__(name, config)
    
    def execute(self, context: Dict[str, Any] = None) -> pd.DataFrame:
        """Execute the action to load data from a SQL database.
        
        Args:
            context: Execution context
            
        Returns:
            The loaded data as a pandas DataFrame
        """
        context = context or {}
        
        # Get parameters from config
        connection_string = self.config.get("connection_string", "")
        query = self.config.get("query", "")
        params = self.config.get("params", None)
        
        # For simplicity, we'll only support SQLite for now
        # A more complete implementation would support multiple database types
        
        # Connect to the database
        conn = sqlite3.connect(connection_string)
        
        # Execute the query
        self.data = pd.read_sql_query(query, conn, params=params)
        
        # Close the connection
        conn.close()
        
        return self.data
