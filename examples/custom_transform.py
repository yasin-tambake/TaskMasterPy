"""
Custom data transformation script for TaskMasterPy.

This script demonstrates how to create a custom transformation
that can be called by the RunPythonScriptAction.
"""
import pandas as pd
import numpy as np


def main(df, columns=None, method="log", **kwargs):
    """Apply a custom transformation to a DataFrame.
    
    Args:
        df: The input DataFrame
        columns: List of columns to transform (default: all numeric columns)
        method: Transformation method (default: 'log')
          Options: 'log', 'sqrt', 'square', 'cube', 'custom'
        **kwargs: Additional parameters for custom transformations
    
    Returns:
        The transformed DataFrame
    """
    # Make a copy of the DataFrame
    result = df.copy()
    
    # If columns is not specified, use all numeric columns
    if columns is None:
        columns = result.select_dtypes(include=np.number).columns.tolist()
    
    # Apply the transformation based on the method
    if method == "log":
        for col in columns:
            # Add a small constant to avoid log(0)
            min_val = result[col].min()
            if min_val <= 0:
                result[col] = np.log(result[col] - min_val + 1)
            else:
                result[col] = np.log(result[col])
    
    elif method == "sqrt":
        for col in columns:
            # Ensure values are non-negative
            min_val = result[col].min()
            if min_val < 0:
                result[col] = np.sqrt(result[col] - min_val)
            else:
                result[col] = np.sqrt(result[col])
    
    elif method == "square":
        for col in columns:
            result[col] = result[col] ** 2
    
    elif method == "cube":
        for col in columns:
            result[col] = result[col] ** 3
    
    elif method == "custom":
        # Get the custom function from kwargs
        custom_func = kwargs.get("custom_func")
        if custom_func is None:
            raise ValueError("custom_func is required for custom method")
        
        for col in columns:
            result[col] = result[col].apply(custom_func)
    
    else:
        raise ValueError(f"Unknown transformation method: {method}")
    
    return result


def add_derived_features(df, config=None):
    """Add derived features to a DataFrame.
    
    Args:
        df: The input DataFrame
        config: Configuration for derived features
    
    Returns:
        The DataFrame with derived features
    """
    config = config or {}
    result = df.copy()
    
    # Add interaction terms
    interaction_terms = config.get("interaction_terms", [])
    for term in interaction_terms:
        if len(term) != 2:
            continue
        
        col1, col2 = term
        if col1 in result.columns and col2 in result.columns:
            result[f"{col1}_{col2}_interaction"] = result[col1] * result[col2]
    
    # Add polynomial features
    poly_columns = config.get("poly_columns", [])
    poly_degree = config.get("poly_degree", 2)
    
    for col in poly_columns:
        if col in result.columns:
            for degree in range(2, poly_degree + 1):
                result[f"{col}_power_{degree}"] = result[col] ** degree
    
    # Add date features
    date_columns = config.get("date_columns", [])
    
    for col in date_columns:
        if col in result.columns:
            # Convert to datetime if not already
            if not pd.api.types.is_datetime64_dtype(result[col]):
                result[col] = pd.to_datetime(result[col], errors='coerce')
            
            # Extract date components
            result[f"{col}_year"] = result[col].dt.year
            result[f"{col}_month"] = result[col].dt.month
            result[f"{col}_day"] = result[col].dt.day
            result[f"{col}_dayofweek"] = result[col].dt.dayofweek
            result[f"{col}_quarter"] = result[col].dt.quarter
    
    return result


if __name__ == "__main__":
    # Example usage
    data = {
        "A": [1, 2, 3, 4, 5],
        "B": [10, 20, 30, 40, 50],
        "C": ["x", "y", "z", "x", "y"],
        "date": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"]
    }
    
    df = pd.DataFrame(data)
    
    # Apply log transformation
    transformed_df = main(df, columns=["A", "B"], method="log")
    print("Log transformation:")
    print(transformed_df)
    
    # Add derived features
    config = {
        "interaction_terms": [["A", "B"]],
        "poly_columns": ["A"],
        "poly_degree": 3,
        "date_columns": ["date"]
    }
    
    derived_df = add_derived_features(df, config)
    print("\nDerived features:")
    print(derived_df)