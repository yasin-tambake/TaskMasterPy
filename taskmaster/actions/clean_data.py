"""
Data cleaning actions for TaskMasterPy.

This module defines actions for cleaning data, such as dropping NA values,
fixing data types, renaming columns, or filtering rows.
"""
from typing import Dict, Any, Optional, List, Union
import pandas as pd

from taskmaster.actions.base import BaseAction


class CleanDataAction(BaseAction):
    """Base class for actions that clean data."""

    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new clean data action.

        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
        """
        super().__init__(name, config)

    def execute(self, context: Dict[str, Any] = None) -> pd.DataFrame:
        """Execute the action to clean data.

        Args:
            context: Execution context

        Returns:
            The cleaned data as a pandas DataFrame
        """
        raise NotImplementedError("Subclasses must implement execute()")


class DropNAAction(CleanDataAction):
    """Action to drop NA values from a DataFrame."""

    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new drop NA action.

        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
                - columns: Columns to check for NA values (default: all)
                - how: How to drop NA values (default: 'any')
                - thresh: Minimum number of non-NA values required
                - subset: Columns to consider when dropping rows
        """
        super().__init__(name, config)

    def execute(self, context: Dict[str, Any] = None) -> pd.DataFrame:
        """Execute the action to drop NA values.

        Args:
            context: Execution context, must contain a DataFrame

        Returns:
            The cleaned data as a pandas DataFrame
        """
        context = context or {}

        # Get the input DataFrame
        df = self._get_input_dataframe(context)

        # Get parameters from config
        columns = self.config.get("columns", None)
        how = self.config.get("how", "any")
        thresh = self.config.get("thresh", None)
        subset = self.config.get("subset", None)

        # If columns is specified, only consider those columns
        if columns:
            df_subset = df[columns]
            df_cleaned = df[~df_subset.isna().any(axis=1)]
        else:
            # Drop NA values - only use how if thresh is None
            if thresh is not None:
                df_cleaned = df.dropna(thresh=thresh, subset=subset)
            else:
                df_cleaned = df.dropna(how=how, subset=subset)

        return df_cleaned

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


class FixDataTypesAction(CleanDataAction):
    """Action to fix data types in a DataFrame."""

    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new fix data types action.

        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
                - column_types: Dictionary mapping column names to data types
                - infer_types: Whether to infer data types (default: False)
        """
        super().__init__(name, config)

    def execute(self, context: Dict[str, Any] = None) -> pd.DataFrame:
        """Execute the action to fix data types.

        Args:
            context: Execution context, must contain a DataFrame

        Returns:
            The cleaned data as a pandas DataFrame
        """
        context = context or {}

        # Get the input DataFrame
        df = self._get_input_dataframe(context)

        # Get parameters from config
        column_types = self.config.get("column_types", {})
        infer_types = self.config.get("infer_types", False)

        # Make a copy of the DataFrame
        df_cleaned = df.copy()

        # Fix data types
        if column_types:
            for column, dtype in column_types.items():
                if column in df_cleaned.columns:
                    try:
                        df_cleaned[column] = df_cleaned[column].astype(dtype)
                    except Exception as e:
                        print(f"Error converting column {column} to {dtype}: {str(e)}")

        # Infer data types if requested
        if infer_types:
            for column in df_cleaned.columns:
                # Try to convert to numeric
                try:
                    df_cleaned[column] = pd.to_numeric(df_cleaned[column])
                except:
                    # Try to convert to datetime
                    try:
                        df_cleaned[column] = pd.to_datetime(df_cleaned[column])
                    except:
                        # Leave as is
                        pass

        return df_cleaned

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


class RenameColumnsAction(CleanDataAction):
    """Action to rename columns in a DataFrame."""

    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new rename columns action.

        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
                - column_map: Dictionary mapping old column names to new ones
        """
        super().__init__(name, config)

    def execute(self, context: Dict[str, Any] = None) -> pd.DataFrame:
        """Execute the action to rename columns.

        Args:
            context: Execution context, must contain a DataFrame

        Returns:
            The cleaned data as a pandas DataFrame
        """
        context = context or {}

        # Get the input DataFrame
        df = self._get_input_dataframe(context)

        # Get parameters from config
        column_map = self.config.get("column_map", {})

        # Rename columns
        df_cleaned = df.rename(columns=column_map)

        return df_cleaned

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


class FilterRowsAction(CleanDataAction):
    """Action to filter rows in a DataFrame."""

    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new filter rows action.

        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
                - filters: List of filter conditions
                  Each filter is a dictionary with keys:
                  - column: Column name
                  - operator: Comparison operator (==, !=, >, <, >=, <=, in, not in)
                  - value: Value to compare against
        """
        super().__init__(name, config)

    def execute(self, context: Dict[str, Any] = None) -> pd.DataFrame:
        """Execute the action to filter rows.

        Args:
            context: Execution context, must contain a DataFrame

        Returns:
            The cleaned data as a pandas DataFrame
        """
        context = context or {}

        # Get the input DataFrame
        df = self._get_input_dataframe(context)

        # Get parameters from config
        filters = self.config.get("filters", [])

        # Apply filters
        df_filtered = df.copy()

        for filter_condition in filters:
            column = filter_condition.get("column")
            operator = filter_condition.get("operator")
            value = filter_condition.get("value")

            if column not in df_filtered.columns:
                print(f"Warning: Column {column} not found in DataFrame")
                continue

            if operator == "==":
                df_filtered = df_filtered[df_filtered[column] == value]
            elif operator == "!=":
                df_filtered = df_filtered[df_filtered[column] != value]
            elif operator == ">":
                df_filtered = df_filtered[df_filtered[column] > value]
            elif operator == "<":
                df_filtered = df_filtered[df_filtered[column] < value]
            elif operator == ">=":
                df_filtered = df_filtered[df_filtered[column] >= value]
            elif operator == "<=":
                df_filtered = df_filtered[df_filtered[column] <= value]
            elif operator == "in":
                df_filtered = df_filtered[df_filtered[column].isin(value)]
            elif operator == "not in":
                df_filtered = df_filtered[~df_filtered[column].isin(value)]
            else:
                print(f"Warning: Unsupported operator {operator}")

        return df_filtered

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
