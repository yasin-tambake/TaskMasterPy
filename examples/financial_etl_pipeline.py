"""
Financial ETL Pipeline using TaskMasterPy

This script demonstrates how to use TaskMasterPy to create an ETL pipeline
specifically for financial data analysis.
"""
import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Add the parent directory to the path so we can import the taskmaster package
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from taskmaster.core.workflow import Workflow
from taskmaster.core.runner import WorkflowRunner
from taskmaster.triggers.time_trigger import TimeTrigger
from taskmaster.actions.load_data import LoadCSVAction
from taskmaster.actions.transform_data import NormalizeAction, AggregateAction
from taskmaster.actions.clean_data import DropNAAction, FixDataTypesAction
from taskmaster.actions.save_data import SaveCSVAction, SaveJSONAction
from taskmaster.actions.notify import ConsoleNotifyAction

# Create sample financial data
def create_financial_data():
    """Create sample financial data for the ETL pipeline."""
    # Create data directory if it doesn't exist
    
    os.makedirs("data", exist_ok=True)
    
    # Generate dates for the past year
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    dates = pd.date_range(start=start_date, end=end_date, freq='B')  # Business days
    
    # Create stock price data for 5 companies
    companies = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']
    
    # Initialize with base prices
    base_prices = {
        'AAPL': 150.0,
        'MSFT': 300.0,
        'GOOGL': 2800.0,
        'AMZN': 3300.0,
        'META': 330.0
    }
    
    # Generate random walk for stock prices
    np.random.seed(42)  # For reproducibility
    
    stock_data = []
    for company in companies:
        price = base_prices[company]
        prices = [price]
        
        for _ in range(1, len(dates)):
            # Random daily return between -3% and +3%
            daily_return = np.random.normal(0.0005, 0.015)
            price = price * (1 + daily_return)
            prices.append(price)
        
        # Create DataFrame for this company
        company_data = pd.DataFrame({
            'date': dates,
            'symbol': company,
            'price': prices,
            'volume': np.random.randint(1000000, 10000000, len(dates))
        })
        
        stock_data.append(company_data)
    
    # Combine all company data
    stock_df = pd.concat(stock_data, ignore_index=True)
    
    # Add some financial metrics
    stock_df['market_cap'] = stock_df['price'] * stock_df['volume'] / 1000000
    stock_df['sector'] = np.random.choice(['Technology', 'Consumer', 'Healthcare'], len(stock_df))
    
    # Add some missing values
    stock_df.loc[np.random.choice(stock_df.index, 20), 'volume'] = np.nan
    
    # Save to CSV
    stock_df.to_csv("data/stock_data.csv", index=False)
    print("Sample financial data created at data/stock_data.csv")

def create_financial_etl_pipeline():
    """Create a financial ETL pipeline workflow."""
    # Create a workflow
    workflow = Workflow(
        name="Financial ETL Pipeline",
        description="Process financial data for analysis and reporting"
    )
    
    # Add a trigger (run once)
    trigger = TimeTrigger(
        name="Daily Update",
        config={"schedule_str": "every 1 day at 09:00"}
    )
    workflow.add_trigger(trigger)
    
    # EXTRACT PHASE
    
    # Extract: Load stock data
    extract_action = LoadCSVAction(
        name="Extract Stock Data",
        config={
            "file_path": "./data/stock_data.csv",
            "parse_dates": ["date"]
        }
    )
    workflow.add_action(extract_action)
    
    # TRANSFORM PHASE
    
    # Clean: Drop NA values
    clean_action = DropNAAction(
        name="Clean Stock Data",
        config={}  # Default behavior is to drop any row with NA
    )
    workflow.add_action(clean_action)
    workflow.add_dependency(clean_action, extract_action)
    
    # Transform: Fix data types
    fix_types_action = FixDataTypesAction(
        name="Fix Data Types",
        config={
            "column_types": {
                "price": "float",
                "volume": "int",
                "market_cap": "float"
            }
        }
    )
    workflow.add_action(fix_types_action)
    workflow.add_dependency(fix_types_action, clean_action)
    
    # Transform: Calculate daily returns
    class CalculateReturnsAction(NormalizeAction):
        """Custom action to calculate daily returns."""
        
        def execute(self, context=None):
            context = context or {}
            df = self._get_input_dataframe(context)
            
            # Make a copy of the DataFrame
            result = df.copy()
            
            # Sort by symbol and date
            result = result.sort_values(['symbol', 'date'])
            
            # Calculate daily returns by symbol
            result['daily_return'] = result.groupby('symbol')['price'].pct_change()
            
            # Calculate 5-day moving average of price
            result['ma_5'] = result.groupby('symbol')['price'].transform(lambda x: x.rolling(window=5).mean())
            
            # Calculate 20-day moving average of price
            result['ma_20'] = result.groupby('symbol')['price'].transform(lambda x: x.rolling(window=20).mean())
            
            # Calculate volatility (standard deviation of returns over 20 days)
            result['volatility'] = result.groupby('symbol')['daily_return'].transform(lambda x: x.rolling(window=20).std())
            
            return result
    
    returns_action = CalculateReturnsAction(
        name="Calculate Returns",
        config={}
    )
    workflow.add_action(returns_action)
    workflow.add_dependency(returns_action, fix_types_action)
    
    # Transform: Aggregate by symbol
    aggregate_action = AggregateAction(
        name="Aggregate by Symbol",
        config={
            "group_by": ["symbol"],
            "aggregations": {
                "price": ["mean", "min", "max", "last"],
                "volume": ["mean", "sum"],
                "daily_return": ["mean", "std"],
                "volatility": ["mean", "last"]
            },
            "reset_index": True
        }
    )
    workflow.add_action(aggregate_action)
    workflow.add_dependency(aggregate_action, returns_action)
    
    # Transform: Aggregate by date for market summary
    market_summary_action = AggregateAction(
        name="Market Summary by Date",
        config={
            "group_by": ["date"],
            "aggregations": {
                "price": ["mean"],
                "volume": ["sum"],
                "market_cap": ["sum"]
            },
            "reset_index": True
        }
    )
    workflow.add_action(market_summary_action)
    workflow.add_dependency(market_summary_action, returns_action)
    
    # LOAD PHASE
    
    # Load: Save processed stock data
    save_stocks_action = SaveCSVAction(
        name="Save Processed Stock Data",
        config={
            "file_path": "./data/processed_stocks.csv",
            "index": False
        }
    )
    workflow.add_action(save_stocks_action)
    workflow.add_dependency(save_stocks_action, returns_action)
    
    # Load: Save stock summary
    save_summary_action = SaveCSVAction(
        name="Save Stock Summary",
        config={
            "file_path": "./data/stock_summary.csv",
            "index": False
        }
    )
    workflow.add_action(save_summary_action)
    workflow.add_dependency(save_summary_action, aggregate_action)
    
    # Load: Save market summary
    save_market_action = SaveCSVAction(
        name="Save Market Summary",
        config={
            "file_path": "./data/market_summary.csv",
            "index": False
        }
    )
    workflow.add_action(save_market_action)
    workflow.add_dependency(save_market_action, market_summary_action)
    
    # Load: Save as JSON for API
    save_json_action = SaveJSONAction(
        name="Save JSON for API",
        config={
            "file_path": "./data/stock_api.json",
            "orient": "records",
            "indent": 2
        }
    )
    workflow.add_action(save_json_action)
    workflow.add_dependency(save_json_action, aggregate_action)
    
    # NOTIFICATION
    
    # Notify: Send notification
    notify_action = ConsoleNotifyAction(
        name="Notify Completion",
        config={
            "message": "Financial ETL pipeline completed successfully",
            "level": "success"
        }
    )
    workflow.add_action(notify_action)
    workflow.add_dependency(notify_action, save_stocks_action)
    workflow.add_dependency(notify_action, save_summary_action)
    workflow.add_dependency(notify_action, save_market_action)
    workflow.add_dependency(notify_action, save_json_action)
    
    return workflow

def run_financial_etl_pipeline():
    """Run the financial ETL pipeline workflow."""
    # Create sample data
    create_financial_data()
    
    # Create the workflow
    workflow = create_financial_etl_pipeline()
    
    # Create a runner
    runner = WorkflowRunner()
    
    # Register the workflow
    runner.register_workflow(workflow)
    
    # Run the workflow
    print(f"Running ETL pipeline: {workflow.name}")
    runner.run_workflow_now(workflow.id)
    print("Financial ETL pipeline completed")

if __name__ == "__main__":
    run_financial_etl_pipeline()
