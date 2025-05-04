"""
Advanced ETL Pipeline using TaskMasterPy

This script demonstrates how to use TaskMasterPy to create a more advanced ETL pipeline
that extracts data from multiple sources, performs complex transformations,
and loads the data into different destinations.
"""
import sys
import os
import pandas as pd
import numpy as np

# Add the parent directory to the path so we can import the taskmaster package
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from taskmaster.core.workflow import Workflow
from taskmaster.core.runner import WorkflowRunner
from taskmaster.triggers.time_trigger import TimeTrigger
from taskmaster.actions.load_data import LoadCSVAction
from taskmaster.actions.api import CallAPIAction
from taskmaster.actions.transform_data import NormalizeAction, AggregateAction, PivotAction, EncodeAction
from taskmaster.actions.clean_data import DropNAAction, FixDataTypesAction, FilterRowsAction
from taskmaster.actions.save_data import SaveCSVAction, SaveJSONAction
from taskmaster.actions.notify import ConsoleNotifyAction

# Create a sample CSV file for demonstration
def create_sample_data():
    """Create sample data files for the ETL pipeline."""
    # Create data directory if it doesn't exist
    os.makedirs("data", exist_ok=True)
    
    # Create a sample sales data CSV
    sales_data = {
        "date": pd.date_range(start="2023-01-01", periods=100),
        "product_id": np.random.randint(1, 11, 100),
        "category": np.random.choice(["Electronics", "Clothing", "Food", "Books"], 100),
        "quantity": np.random.randint(1, 50, 100),
        "price": np.random.uniform(10, 1000, 100).round(2),
        "customer_id": np.random.randint(1, 21, 100)
    }
    
    sales_df = pd.DataFrame(sales_data)
    
    # Add some missing values
    sales_df.loc[np.random.choice(sales_df.index, 10), "quantity"] = np.nan
    sales_df.loc[np.random.choice(sales_df.index, 5), "price"] = np.nan
    
    # Save to CSV
    sales_df.to_csv("data/sales_data.csv", index=False)
    print("Sample sales data created at data/sales_data.csv")

def create_etl_pipeline():
    """Create an advanced ETL pipeline workflow."""
    # Create a workflow
    workflow = Workflow(
        name="Advanced ETL Pipeline",
        description="Extract data from multiple sources, transform, and load to different destinations"
    )
    
    # Add a trigger (run once)
    trigger = TimeTrigger(
        name="Manual Trigger",
        config={"schedule_str": "every 1 hour"}
    )
    workflow.add_trigger(trigger)
    
    # EXTRACT PHASE
    
    # Extract 1: Load CSV data
    extract_csv_action = LoadCSVAction(
        name="Extract Sales Data",
        config={
            "file_path": "./data/sales_data.csv",
            "parse_dates": ["date"]
        }
    )
    workflow.add_action(extract_csv_action)
    
    # Extract 2: Call API to get customer data
    extract_api_action = CallAPIAction(
        name="Extract Customer Data",
        config={
            "url": "https://jsonplaceholder.typicode.com/users",
            "method": "GET",
            "return_type": "dataframe"
        }
    )
    workflow.add_action(extract_api_action)
    
    # TRANSFORM PHASE - SALES DATA
    
    # Clean 1: Drop NA values from sales data
    clean_sales_action = DropNAAction(
        name="Clean Sales Data",
        config={}  # Default behavior is to drop any row with NA
    )
    workflow.add_action(clean_sales_action)
    workflow.add_dependency(clean_sales_action, extract_csv_action)
    
    # Transform 1: Fix data types in sales data
    fix_types_action = FixDataTypesAction(
        name="Fix Data Types",
        config={
            "column_types": {
                "product_id": "int",
                "quantity": "int",
                "price": "float",
                "customer_id": "int"
            }
        }
    )
    workflow.add_action(fix_types_action)
    workflow.add_dependency(fix_types_action, clean_sales_action)
    
    # Transform 2: Filter high-value sales
    filter_sales_action = FilterRowsAction(
        name="Filter High-Value Sales",
        config={
            "filters": [
                {"column": "price", "operator": ">", "value": 100}
            ]
        }
    )
    workflow.add_action(filter_sales_action)
    workflow.add_dependency(filter_sales_action, fix_types_action)
    
    # Transform 3: Encode categorical data
    encode_action = EncodeAction(
        name="Encode Categories",
        config={
            "columns": ["category"],
            "method": "onehot",
            "drop_first": True
        }
    )
    workflow.add_action(encode_action)
    workflow.add_dependency(encode_action, filter_sales_action)
    
    # Transform 4: Aggregate sales by date and category
    aggregate_action = AggregateAction(
        name="Aggregate Sales",
        config={
            "group_by": ["date", "category"],
            "aggregations": {
                "quantity": ["sum"],
                "price": ["mean", "sum"]
            },
            "reset_index": True
        }
    )
    workflow.add_action(aggregate_action)
    workflow.add_dependency(aggregate_action, encode_action)
    
    # Transform 5: Create a pivot table of sales by date and category
    pivot_action = PivotAction(
        name="Create Sales Pivot",
        config={
            "index": "date",
            "columns": "category",
            "values": "price_sum",
            "aggfunc": "sum",
            "fill_value": 0
        }
    )
    workflow.add_action(pivot_action)
    workflow.add_dependency(pivot_action, aggregate_action)
    
    # TRANSFORM PHASE - CUSTOMER DATA
    
    # Transform 6: Normalize customer data
    normalize_action = NormalizeAction(
        name="Normalize Customer Data",
        config={
            "columns": ["id"],
            "method": "minmax"
        }
    )
    workflow.add_action(normalize_action)
    workflow.add_dependency(normalize_action, extract_api_action)
    
    # LOAD PHASE
    
    # Load 1: Save aggregated sales data to CSV
    save_agg_action = SaveCSVAction(
        name="Save Aggregated Sales",
        config={
            "file_path": "./data/aggregated_sales.csv",
            "index": False
        }
    )
    workflow.add_action(save_agg_action)
    workflow.add_dependency(save_agg_action, aggregate_action)
    
    # Load 2: Save pivot table to CSV
    save_pivot_action = SaveCSVAction(
        name="Save Sales Pivot",
        config={
            "file_path": "./data/sales_pivot.csv",
            "index": True
        }
    )
    workflow.add_action(save_pivot_action)
    workflow.add_dependency(save_pivot_action, pivot_action)
    
    # Load 3: Save customer data to JSON
    save_customers_action = SaveJSONAction(
        name="Save Customer Data",
        config={
            "file_path": "./data/customers.json",
            "orient": "records",
            "indent": 2
        }
    )
    workflow.add_action(save_customers_action)
    workflow.add_dependency(save_customers_action, normalize_action)
    
    # NOTIFICATION
    
    # Notify: Send notification
    notify_action = ConsoleNotifyAction(
        name="Notify Completion",
        config={
            "message": "Advanced ETL pipeline completed successfully",
            "level": "success"
        }
    )
    workflow.add_action(notify_action)
    workflow.add_dependency(notify_action, save_agg_action)
    workflow.add_dependency(notify_action, save_pivot_action)
    workflow.add_dependency(notify_action, save_customers_action)
    
    return workflow

def run_etl_pipeline():
    """Run the ETL pipeline workflow."""
    # Create sample data
    create_sample_data()
    
    # Create the workflow
    workflow = create_etl_pipeline()
    
    # Create a runner
    runner = WorkflowRunner()
    
    # Register the workflow
    runner.register_workflow(workflow)
    
    # Run the workflow
    print(f"Running ETL pipeline: {workflow.name}")
    runner.run_workflow_now(workflow.id)
    print("ETL pipeline completed")

if __name__ == "__main__":
    run_etl_pipeline()
