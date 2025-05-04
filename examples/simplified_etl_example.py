"""
Simplified ETL Example using TaskMasterPy

This script demonstrates how to use TaskMasterPy with simplified imports
to create an ETL (Extract, Transform, Load) pipeline.

Author: Adriella
"""

# Import TaskMasterPy with an alias
import taskmaster as tm
import time

def main():
    # Create a workflow
    workflow = tm.core.workflow.Workflow(
        name="Simplified ETL Workflow", 
        description="Extract, Transform, Load pipeline with simplified imports"
    )
    
    # Add a time trigger
    time_trigger = tm.triggers.time_trigger.TimeTrigger(
        name="Hourly Schedule",
        config={
            "schedule_str": "every 1 hour"
        }
    )
    workflow.add_trigger(time_trigger)
    
    # Extract: Load data from CSV
    extract_action = tm.actions.load_data.LoadCSVAction(
        name="Extract Data",
        config={
            "file_path": "data/sales_data.csv",
            "parse_dates": ["date"]
        }
    )
    
    # Transform: Clean the data
    clean_action = tm.actions.clean_data.DropNAAction(
        name="Clean Data",
        config={}
    )
    
    # Transform: Fix data types
    fix_types_action = tm.actions.clean_data.FixDataTypesAction(
        name="Fix Data Types",
        config={
            "column_types": {
                "amount": "float",
                "quantity": "int"
            }
        }
    )
    
    # Transform: Aggregate the data
    aggregate_action = tm.actions.transform_data.AggregateAction(
        name="Aggregate Data",
        config={
            "group_by": ["date", "product_category"],
            "aggregations": {
                "amount": "sum",
                "quantity": "sum"
            }
        }
    )
    
    # Load: Save the transformed data
    load_action = tm.actions.save_data.SaveCSVAction(
        name="Load Data",
        config={
            "file_path": "data/sales_summary.csv",
            "index": False
        }
    )
    
    # Notify completion
    notify_action = tm.actions.notify.ConsoleNotifyAction(
        name="Notify ETL Completion",
        config={
            "message": "ETL process completed successfully!",
            "level": "success"
        }
    )
    
    # Add actions to the workflow
    workflow.add_action(extract_action)
    workflow.add_action(clean_action)
    workflow.add_action(fix_types_action)
    workflow.add_action(aggregate_action)
    workflow.add_action(load_action)
    workflow.add_action(notify_action)
    
    # Set up dependencies
    workflow.add_dependency(clean_action, extract_action)
    workflow.add_dependency(fix_types_action, clean_action)
    workflow.add_dependency(aggregate_action, fix_types_action)
    workflow.add_dependency(load_action, aggregate_action)
    workflow.add_dependency(notify_action, load_action)
    
    # Create a workflow runner
    runner = tm.core.runner.WorkflowRunner()
    
    # Register the workflow
    runner.register_workflow(workflow)
    
    # For demonstration, run the workflow immediately instead of waiting for the trigger
    print("Running ETL workflow immediately...")
    result = runner.run_workflow_now(workflow.id)
    
    print("Workflow execution completed!")
    
    # Alternatively, you could start the workflow and let it run on schedule:
    # runner.start_workflow(workflow.id)
    # print("ETL workflow started. It will run every hour.")
    # print("Press Ctrl+C to stop...")
    # try:
    #     while True:
    #         time.sleep(1)
    # except KeyboardInterrupt:
    #     runner.stop_workflow(workflow.id)
    #     print("ETL workflow stopped.")

if __name__ == "__main__":
    main()
