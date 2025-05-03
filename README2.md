# TaskMasterPy

A Python-based open-source automation framework focused on data operations, designed for data scientists.

## Overview

TaskMasterPy is an extensible, CLI-first, event-driven automation engine that allows creating workflows via YAML/JSON config files or Python APIs. The goal is to simplify automating tasks such as data ingestion, cleaning, transformation, scheduling, and notification via triggers and actions.

## Architecture

TaskMasterPy follows a modular architecture with these key components:

### Core Components

- **Workflow**: Central class that manages triggers and actions in a directed acyclic graph (DAG)
- **WorkflowRunner**: Handles workflow execution and lifecycle management
- **Autopilot**: Simplified API for common data operations

### Actions

Actions are the building blocks of workflows, each performing a specific task:

- **Data Loading**: Load data from CSV, JSON, Excel, SQL
- **Data Cleaning**: Drop NA values, fix data types, rename columns, filter rows
- **Data Transformation**: Normalize, aggregate, pivot, encode categorical data
- **Data Saving**: Save to CSV, JSON, Excel, SQL
- **API Integration**: Make HTTP requests, send webhooks
- **Script Execution**: Run Python or shell scripts
- **Notifications**: Console, system, email notifications

### Triggers

Triggers initiate workflow execution based on events:

- **Time Triggers**: Schedule-based execution
- **File Triggers**: File system events (creation, modification)
- **API Triggers**: Webhook endpoints

### Utilities

- **Config Utilities**: Load and parse workflow configurations
- **Logging**: Track workflow execution
- **Validators**: Ensure configuration correctness

## Technical Implementation

TaskMasterPy is built using:

1. **Python's Type Hints**: For better code documentation and IDE support
2. **Object-Oriented Design**: With inheritance for extensibility
3. **Pandas**: For data manipulation
4. **YAML/JSON**: For configuration
5. **Design Patterns**:
   - Factory Pattern: Creating actions from configuration
   - Strategy Pattern: Different action implementations
   - Observer Pattern: For trigger callbacks
   - Dependency Injection: For workflow context

## Usage Examples

### YAML Configuration

```yaml
name: Data Processing Workflow
description: Process a CSV file

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
      file_path: ./data/input.csv
  
  - id: clean_data
    type: drop_na
    name: Clean Data
    depends_on:
      - load_data
  
  - id: save_data
    type: save_csv
    name: Save Cleaned Data
    config:
      file_path: ./data/output.csv
    depends_on:
      - clean_data
```

### Python API

```python
from taskmaster.core.workflow import Workflow
from taskmaster.core.runner import WorkflowRunner
from taskmaster.triggers.time_trigger import TimeTrigger
from taskmaster.actions.load_data import LoadCSVAction
from taskmaster.actions.clean_data import DropNAAction
from taskmaster.actions.save_data import SaveCSVAction

# Create workflow
workflow = Workflow(name="Data Processing")

# Add trigger
trigger = TimeTrigger(name="Hourly", config={"schedule_str": "every 1 hour"})
workflow.add_trigger(trigger)

# Add actions
load_action = LoadCSVAction(name="Load Data", config={"file_path": "./data/input.csv"})
clean_action = DropNAAction(name="Clean Data")
save_action = SaveCSVAction(name="Save Data", config={"file_path": "./data/output.csv"})

workflow.add_action(load_action)
workflow.add_action(clean_action)
workflow.add_action(save_action)

# Add dependencies
workflow.add_dependency(clean_action, load_action)
workflow.add_dependency(save_action, clean_action)

# Run workflow
runner = WorkflowRunner()
runner.add_workflow(workflow)
runner.run_workflow(workflow.id)
```

### Autopilot Mode

```python
from taskmaster import autopilot

# One-liner for simple data processing
autopilot(
    data_path="data.csv", 
    output_path="processed_data.csv", 
    normalize=True, 
    drop_na=True
)
```

## Project Structure

```
TaskMasterPy/
├── taskmaster/
│   ├── __init__.py
│   ├── autopilot.py
│   ├── main.py
│   ├── actions/
│   │   ├── __init__.py
│   │   ├── base.py
│   │   ├── load_data.py
│   │   ├── clean_data.py
│   │   ├── transform_data.py
│   │   ├── save_data.py
│   │   ├── api.py
│   │   ├── script.py
│   │   └── notify.py
│   ├── triggers/
│   │   ├── __init__.py
│   │   ├── base.py
│   │   └── time_trigger.py
│   ├── core/
│   │   ├── __init__.py
│   │   ├── workflow.py
│   │   └── runner.py
│   ├── cli/
│   │   ├── __init__.py
│   │   └── commands.py
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── logging.py
│   │   └── validators.py
│   └── plugins/
│       ├── __init__.py
│       └── loader.py
├── examples/
│   ├── simple_workflow.yaml
│   ├── api_workflow.yaml
│   ├── etl_pipeline.py
│   ├── custom_transform.py
│   └── financial_etl_pipeline.py
├── setup.py
└── README.md
```

## Installation

```bash
pip install taskmasterpy
```

## CLI Commands

- `taskmaster run <config_file>`: Run a workflow
- `taskmaster list-workflows`: List available workflows
- `taskmaster validate <config_file>`: Validate a workflow configuration
- `taskmaster trigger-now <workflow_id>`: Manually trigger a workflow

## Dependencies

- pandas>=1.0.0
- numpy>=1.18.0
- pyyaml>=5.1
- typer>=0.3.0
- rich>=10.0.0
- schedule>=1.0.0
- requests>=2.25.0
- watchdog>=2.1.0
- jmespath>=0.10.0

## License

This project is licensed under the MIT License.