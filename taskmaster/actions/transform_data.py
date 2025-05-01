"""
Data transformation actions for TaskMasterPy.

This module defines actions for transforming data, such as normalizing,
aggregating, grouping, pivoting, or encoding.
"""
from typing import Dict, Any, Optional, List, Union
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, MinMaxScaler, OneHotEncoder, LabelEncoder

from taskmaster.actions.base import BaseAction


class TransformDataAction(BaseAction):
    """Base class for actions that transform data."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new transform data action.
        
        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
        """
        super().__init__(name, config)
    
    def execute(self, context: Dict[str, Any] = None) -> pd.DataFrame:
        """Execute the action to transform data.
        
        Args:
            context: Execution context
            
        Returns:
            The transformed data as a pandas DataFrame
        """
        raise NotImplementedError("Subclasses must implement execute()")
    
    def _get_input_dataframe(self, context: Dict[str, Any]) -> pd.DataFrame:
        """Get the input DataFrame from the context.
        
        Args:
            context: Execution context
            
        Returns:
            The input DataFrame
        """
        # Check if there's a specific input action ID
        input_action_id = self.config.get("input_action_id")
        
        # If there's a specific input action, use its result
        if input_action_id and input_action_id in context:
            df = context[input_action_id]
        else:
            # Otherwise, try to find a DataFrame in the context
            # First, check if any of our dependencies produced a DataFrame
            for dep in self.dependencies:
                if dep.id in context and isinstance(context[dep.id], pd.DataFrame):
                    df = context[dep.id]
                    break
            else:
                # If no dependency has a DataFrame, look for any DataFrame in the context
                for key, value in context.items():
                    if isinstance(value, pd.DataFrame):
                        df = value
                        break
                else:
                    raise ValueError("No DataFrame found in context")
        
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"Expected a DataFrame, got {type(df)}")
        
        return df


class NormalizeAction(TransformDataAction):
    """Action to normalize data using various methods."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new normalize action.
        
        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
                - columns: List of columns to normalize (default: all numeric columns)
                - method: Normalization method (default: 'zscore')
                  Options: 'zscore', 'minmax', 'robust', 'log'
                - inplace: Whether to modify the original DataFrame (default: False)
        """
        super().__init__(name, config)
    
    def execute(self, context: Dict[str, Any] = None) -> pd.DataFrame:
        """Execute the action to normalize data.
        
        Args:
            context: Execution context, must contain a DataFrame
            
        Returns:
            The normalized data as a pandas DataFrame
        """
        context = context or {}
        
        # Get the input DataFrame
        df = self._get_input_dataframe(context)
        
        # Get parameters from config
        columns = self.config.get("columns")
        method = self.config.get("method", "zscore")
        inplace = self.config.get("inplace", False)
        
        # Make a copy of the DataFrame if not inplace
        if not inplace:
            df = df.copy()
        
        # If columns is not specified, use all numeric columns
        if columns is None:
            columns = df.select_dtypes(include=np.number).columns.tolist()
        
        # Check if columns exist in the DataFrame
        for col in columns:
            if col not in df.columns:
                raise ValueError(f"Column {col} not found in DataFrame")
        
        # Apply normalization based on the method
        if method == "zscore":
            # Z-score normalization (mean=0, std=1)
            scaler = StandardScaler()
            df[columns] = scaler.fit_transform(df[columns])
        
        elif method == "minmax":
            # Min-max normalization (range [0, 1])
            scaler = MinMaxScaler()
            df[columns] = scaler.fit_transform(df[columns])
        
        elif method == "robust":
            # Robust normalization (median=0, IQR=1)
            for col in columns:
                median = df[col].median()
                q1 = df[col].quantile(0.25)
                q3 = df[col].quantile(0.75)
                iqr = q3 - q1
                if iqr > 0:
                    df[col] = (df[col] - median) / iqr
        
        elif method == "log":
            # Log normalization
            for col in columns:
                # Add a small constant to avoid log(0)
                min_val = df[col].min()
                if min_val <= 0:
                    df[col] = np.log(df[col] - min_val + 1)
                else:
                    df[col] = np.log(df[col])
        
        else:
            raise ValueError(f"Unknown normalization method: {method}")
        
        return df


class AggregateAction(TransformDataAction):
    """Action to aggregate data using various methods."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new aggregate action.
        
        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
                - group_by: Column(s) to group by
                - aggregations: Dictionary mapping columns to aggregation functions
                  Example: {"sales": "sum", "price": ["mean", "max"]}
                - reset_index: Whether to reset the index (default: True)
        """
        super().__init__(name, config)
    
    def execute(self, context: Dict[str, Any] = None) -> pd.DataFrame:
        """Execute the action to aggregate data.
        
        Args:
            context: Execution context, must contain a DataFrame
            
        Returns:
            The aggregated data as a pandas DataFrame
        """
        context = context or {}
        
        # Get the input DataFrame
        df = self._get_input_dataframe(context)
        
        # Get parameters from config
        group_by = self.config.get("group_by")
        aggregations = self.config.get("aggregations", {})
        reset_index = self.config.get("reset_index", True)
        
        if not group_by:
            raise ValueError("group_by parameter is required")
        
        if not aggregations:
            raise ValueError("aggregations parameter is required")
        
        # Group by the specified column(s)
        grouped = df.groupby(group_by)
        
        # Apply aggregations
        result = grouped.agg(aggregations)
        
        # Reset index if requested
        if reset_index:
            result = result.reset_index()
        
        return result


class PivotAction(TransformDataAction):
    """Action to pivot data."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new pivot action.
        
        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
                - index: Column(s) to use as index
                - columns: Column(s) to use as columns
                - values: Column(s) to use as values
                - aggfunc: Aggregation function to use (default: 'mean')
                - fill_value: Value to use for missing values
                - pivot_type: Type of pivot to perform (default: 'pivot_table')
                  Options: 'pivot_table', 'pivot', 'crosstab'
        """
        super().__init__(name, config)
    
    def execute(self, context: Dict[str, Any] = None) -> pd.DataFrame:
        """Execute the action to pivot data.
        
        Args:
            context: Execution context, must contain a DataFrame
            
        Returns:
            The pivoted data as a pandas DataFrame
        """
        context = context or {}
        
        # Get the input DataFrame
        df = self._get_input_dataframe(context)
        
        # Get parameters from config
        index = self.config.get("index")
        columns = self.config.get("columns")
        values = self.config.get("values")
        aggfunc = self.config.get("aggfunc", "mean")
        fill_value = self.config.get("fill_value")
        pivot_type = self.config.get("pivot_type", "pivot_table")
        
        if not index:
            raise ValueError("index parameter is required")
        
        if not columns:
            raise ValueError("columns parameter is required")
        
        # Perform the pivot operation based on the type
        if pivot_type == "pivot_table":
            if not values:
                raise ValueError("values parameter is required for pivot_table")
            
            result = pd.pivot_table(
                df,
                values=values,
                index=index,
                columns=columns,
                aggfunc=aggfunc,
                fill_value=fill_value
            )
        
        elif pivot_type == "pivot":
            if not values:
                raise ValueError("values parameter is required for pivot")
            
            result = df.pivot(
                index=index,
                columns=columns,
                values=values
            )
            
            if fill_value is not None:
                result = result.fillna(fill_value)
        
        elif pivot_type == "crosstab":
            # For crosstab, index and columns should be column names in the DataFrame
            result = pd.crosstab(
                df[index],
                df[columns],
                values=df[values] if values else None,
                aggfunc=aggfunc if values else None,
                normalize=self.config.get("normalize", False)
            )
            
            if fill_value is not None:
                result = result.fillna(fill_value)
        
        else:
            raise ValueError(f"Unknown pivot type: {pivot_type}")
        
        return result


class EncodeAction(TransformDataAction):
    """Action to encode categorical data."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new encode action.
        
        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
                - columns: List of columns to encode
                - method: Encoding method (default: 'onehot')
                  Options: 'onehot', 'label', 'ordinal', 'dummy'
                - drop_first: Whether to drop the first category (default: False)
                - handle_unknown: How to handle unknown categories (default: 'error')
                  Options: 'error', 'ignore'
                - inplace: Whether to modify the original DataFrame (default: False)
        """
        super().__init__(name, config)
    
    def execute(self, context: Dict[str, Any] = None) -> pd.DataFrame:
        """Execute the action to encode categorical data.
        
        Args:
            context: Execution context, must contain a DataFrame
            
        Returns:
            The encoded data as a pandas DataFrame
        """
        context = context or {}
        
        # Get the input DataFrame
        df = self._get_input_dataframe(context)
        
        # Get parameters from config
        columns = self.config.get("columns")
        method = self.config.get("method", "onehot")
        drop_first = self.config.get("drop_first", False)
        handle_unknown = self.config.get("handle_unknown", "error")
        inplace = self.config.get("inplace", False)
        
        if not columns:
            raise ValueError("columns parameter is required")
        
        # Make a copy of the DataFrame if not inplace
        if not inplace:
            df = df.copy()
        
        # Apply encoding based on the method
        if method == "onehot":
            # One-hot encoding
            encoder = OneHotEncoder(
                sparse=False,
                drop="first" if drop_first else None,
                handle_unknown=handle_unknown
            )
            
            # Fit and transform the data
            encoded = encoder.fit_transform(df[columns])
            
            # Create a DataFrame with the encoded data
            encoded_df = pd.DataFrame(
                encoded,
                columns=encoder.get_feature_names_out(columns),
                index=df.index
            )
            
            # Drop the original columns and add the encoded ones
            df = df.drop(columns, axis=1)
            df = pd.concat([df, encoded_df], axis=1)
        
        elif method == "label":
            # Label encoding
            for col in columns:
                encoder = LabelEncoder()
                df[col] = encoder.fit_transform(df[col])
        
        elif method == "ordinal":
            # Ordinal encoding (similar to label encoding but with specified order)
            for col in columns:
                categories = self.config.get(f"categories_{col}")
                if not categories:
                    raise ValueError(f"categories_{col} parameter is required for ordinal encoding")
                
                # Create a mapping from category to ordinal value
                mapping = {cat: i for i, cat in enumerate(categories)}
                
                # Apply the mapping
                df[col] = df[col].map(mapping)
                
                # Handle unknown values
                if handle_unknown == "ignore":
                    df[col] = df[col].fillna(-1)
        
        elif method == "dummy":
            # Dummy encoding (pandas get_dummies)
            dummy_df = pd.get_dummies(
                df[columns],
                drop_first=drop_first,
                prefix=columns
            )
            
            # Drop the original columns and add the dummy ones
            df = df.drop(columns, axis=1)
            df = pd.concat([df, dummy_df], axis=1)
        
        else:
            raise ValueError(f"Unknown encoding method: {method}")
        
        return df
