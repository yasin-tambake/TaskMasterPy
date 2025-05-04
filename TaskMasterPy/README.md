# TaskMasterPy

A Python-based open-source automation framework focused on data operations, designed for data scientists.

## Overview

TaskMasterPy is an extensible, CLI-first, event-driven automation engine that allows creating workflows via YAML/JSON config files or Python APIs. The goal is to simplify automating tasks such as data ingestion, cleaning, transformation, scheduling, and notification via triggers and actions.

## Features

- **Event-driven architecture**: Define workflows that respond to various triggers
- **Data-focused actions**: Built-in actions for common data operations
- **YAML/JSON configuration**: Define workflows without writing code
- **Python API**: Programmatically create and run workflows
- **Plugin system**: Extend with custom triggers and actions
- **CLI interface**: Run and manage workflows from the command line
- **Autopilot mode**: One-liner for simple data processing tasks
- **Database storage**: Store and manage workflows in a persistent database

## Installation

```bash
pip install taskmasterpy
```

## Quick Start

### Using the CLI

1. Create a workflow configuration file:

```yaml
name: CSV File Monitor Workflow
description: Monitor a directory for new CSV files, process them, and save the results

triggers:
  - type: file
    name: CSV File Watcher
    config:
      path: ./data
      recursive: true
      patterns:
        - "*.csv"
      event_types:
        - created
        - modified

actions:
  - id: load_csv
    type: load_csv
    name: Load CSV File
    config:
      file_path: "{event_data.path}"

  - id: clean_data
    type: drop_na
    name: Clean Data
    depends_on:
      - load_csv

  - id: save_json
    type: save_json
    name: Save as JSON
    config:
      file_path: "./data/processed/{event_data.path|basename|replace('.csv', '.json')}"
    depends_on:
      - clean_data
```

2. Run the workflow:

```bash
taskmaster run workflow.yaml
```

### Using the Python API

```python
from taskmaster import autopilot

# One-liner to process a CSV file
autopilot(data_path="data.csv", output_path="processed_data.csv", normalize=True)

# Or create a custom workflow
from taskmaster.core.workflow import Workflow
from taskmaster.core.runner import WorkflowRunner
from taskmaster.triggers.time_trigger import TimeTrigger
from taskmaster.actions.load_data import LoadCSVAction
from taskmaster.actions.save_data import SaveCSVAction

# Create a workflow
workflow = Workflow(name="My Workflow")

# Add a trigger
trigger = TimeTrigger(name="Hourly", config={"schedule_str": "every 1 hour"})
workflow.add_trigger(trigger)

# Add actions
load_action = LoadCSVAction(name="Load Data", config={"file_path": "data.csv"})
save_action = SaveCSVAction(name="Save Data", config={"file_path": "processed.csv"})

workflow.add_action(load_action)
workflow.add_action(save_action)
workflow.add_dependency(save_action, load_action)

# Run the workflow
runner = WorkflowRunner()
runner.register_workflow(workflow)
runner.run_workflow_now(workflow.id)
```

## Trigger Types

- **TimeTrigger**: Schedule workflows using cron expressions or intervals
- **FileTrigger**: Watch for file system changes
- **APIPollTrigger**: Poll APIs for changes
- **WebhookTrigger**: Respond to incoming webhook requests
- **DBTrigger**: Trigger on database changes

## Action Types

### Data Loading
- **LoadCSVAction**: Load data from CSV files
- **LoadJSONAction**: Load data from JSON files
- **LoadExcelAction**: Load data from Excel files
- **LoadSQLAction**: Load data from SQL databases

### Data Cleaning
- **DropNAAction**: Remove rows with missing values
- **FixDataTypesAction**: Convert columns to appropriate data types
- **RenameColumnsAction**: Rename columns in a DataFrame
- **FilterRowsAction**: Filter rows based on conditions

### Data Transformation
- **NormalizeAction**: Normalize data using various methods (z-score, min-max, etc.)
- **AggregateAction**: Aggregate data using groupby operations
- **PivotAction**: Create pivot tables and cross-tabulations
- **EncodeAction**: Encode categorical data (one-hot, label, ordinal)

### Data Saving
- **SaveCSVAction**: Save data to CSV files
- **SaveJSONAction**: Save data to JSON files
- **SaveExcelAction**: Save data to Excel files
- **SaveSQLAction**: Save data to SQL databases

### API Integration
- **CallAPIAction**: Make HTTP requests to API endpoints
- **WebhookAction**: Send webhooks to external services

### Script Execution
- **RunPythonScriptAction**: Run custom Python scripts
- **RunShellScriptAction**: Run shell scripts

### Notifications
- **ConsoleNotifyAction**: Display notifications in the console
- **SystemNotifyAction**: Send system notifications
- **SendEmailAction**: Send email notifications

## Example Workflows

TaskMasterPy comes with several example workflows to help you get started:

### Database Storage Example

```python
from taskmaster.core.workflow import Workflow
from taskmaster.actions.load_data import LoadCSVAction
from taskmaster.actions.clean_data import DropNAAction
from taskmaster.actions.save_data import SaveCSVAction
from taskmaster.storage.db_storage import WorkflowStorage

# Create a workflow
workflow = Workflow(name="Sample DB Workflow")

# Add actions
load_action = LoadCSVAction(name="Load Data", config={"file_path": "data.csv"})
clean_action = DropNAAction(name="Clean Data")
save_action = SaveCSVAction(name="Save Data", config={"file_path": "cleaned.csv"})

# Add dependencies
workflow.add_action(load_action)
workflow.add_action(clean_action)
workflow.add_action(save_action)
workflow.add_dependency(clean_action, load_action)
workflow.add_dependency(save_action, clean_action)

# Save to database
storage = WorkflowStorage()
storage.save_workflow(workflow.id, {
    "id": workflow.id,
    "name": workflow.name,
    "description": workflow.description,
    "actions": [
        # Action configurations...
    ]
})

# Load from database
loaded_workflow = storage.get_workflow_instance(workflow.id)
```

### Basic Data Processing

```yaml
name: Example Data Processing Workflow
description: A simple workflow to process a CSV file

triggers:
  - type: time
    name: Manual Trigger
    config:
      schedule_str: every 1 hour

actions:
  - id: load_data
    type: load_csv
    name: Load CSV Data
    config:
      file_path: ./data/test.csv

  - id: clean_data
    type: drop_na
    name: Clean Data
    depends_on:
      - load_data

  - id: save_data
    type: save_csv
    name: Save Cleaned Data
    config:
      file_path: ./data/example_cleaned.csv
    depends_on:
      - clean_data

  - id: notify
    type: console_notify
    name: Notify Completion
    config:
      message: "Example workflow completed successfully"
    depends_on:
      - save_data
```

### File Monitoring and Transformation

```yaml
name: File Transform Workflow
description: A workflow that watches for CSV files, transforms the data, and saves the result

triggers:
  - type: file
    name: CSV File Watcher
    config:
      path: ./data
      patterns: ["*.csv"]
      event_types: ["created", "modified"]

actions:
  - id: load_data
    type: load_csv
    name: Load CSV Data
    config:
      file_path: ${event.path}

  - id: transform_data
    type: normalize
    name: Normalize Data
    config:
      method: minmax
    depends_on:
      - load_data

  - id: save_data
    type: save_csv
    name: Save Transformed Data
    config:
      file_path: ${event.path.replace('.csv', '_transformed.csv')}
    depends_on:
      - transform_data
```

### API Integration

```yaml
name: API Integration Workflow
description: A workflow that fetches data from an API, processes it, and sends results to another API

triggers:
  - type: time
    name: Daily Trigger
    config:
      schedule_str: every 1 day at 08:00

actions:
  - id: fetch_data
    type: call_api
    name: Fetch Data from API
    config:
      url: "https://jsonplaceholder.typicode.com/posts"
      method: "GET"
      return_type: "dataframe"

  - id: transform_data
    type: normalize
    name: Normalize Data
    config:
      columns: ["id", "userId"]
      method: minmax
    depends_on:
      - fetch_data

  - id: send_data
    type: call_api
    name: Send Data to API
    config:
      url: "https://httpbin.org/post"
      method: "POST"
      json: ${transform_data.to_dict(orient='records')}
    depends_on:
      - transform_data
```

### Custom Script Execution

```yaml
name: Custom Script Workflow
description: A workflow that uses a custom Python script for data transformation

triggers:
  - type: time
    name: Hourly Trigger
    config:
      schedule_str: every 1 hour

actions:
  - id: load_data
    type: load_csv
    name: Load CSV Data
    config:
      file_path: ./data/test.csv

  - id: run_script
    type: run_python_script
    name: Run Custom Transformation
    config:
      script_path: ./examples/custom_transform.py
      function_name: main
      kwargs:
        method: sqrt
      pass_context: true
      return_dataframe: true
    depends_on:
      - load_data

  - id: save_data
    type: save_csv
    name: Save Transformed Data
    config:
      file_path: ./data/test_script_transformed.csv
    depends_on:
      - run_script
```

## CLI Commands

### Workflow Management
- `taskmaster run <config_file>`: Run a workflow from a file or database
- `taskmaster list-workflows`: List available workflows from files and database
- `taskmaster validate <config_file>`: Validate a workflow configuration
- `taskmaster trigger-now <workflow_id>`: Manually trigger a workflow

### Database Operations
- `taskmaster db list`: List all workflows in the database
- `taskmaster db import <file_path>`: Import a workflow from a file into the database
- `taskmaster db export <workflow_id> <file_path>`: Export a workflow from the database to a file
- `taskmaster db delete <workflow_id>`: Delete a workflow from the database
- `taskmaster db run <workflow_id>`: Run a workflow from the database

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
