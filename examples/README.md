# TaskMasterPy Examples

This directory contains example scripts demonstrating how to use the TaskMasterPy library for various data processing and automation tasks.

## Basic Examples

1. **simple_taskmaster_example.py**
   - A simple example showing how to use TaskMasterPy with the `import taskmaster as tm` approach
   - Demonstrates basic workflow creation, adding actions, and running the workflow

2. **autopilot_example.py**
   - Shows how to use the autopilot feature for quick and simple data processing
   - Demonstrates different configuration options for autopilot

## ETL Examples

3. **simplified_etl_example.py**
   - Demonstrates a complete ETL (Extract, Transform, Load) pipeline
   - Uses simplified imports with the `import taskmaster as tm` approach
   - Shows how to extract data from CSV, transform it, and load it to a new file

4. **scheduled_etl_example.py**
   - Shows how to create an ETL workflow that runs on a schedule
   - Demonstrates time-based triggers and keeping a script running to execute scheduled tasks

## Advanced Examples

5. **file_monitoring_example.py**
   - Demonstrates how to monitor a directory for file changes
   - Shows how to process files automatically when they are created or modified

6. **api_data_processing.py**
   - Shows how to fetch data from an API and process it
   - Demonstrates creating a custom action to convert API responses to DataFrames

7. **custom_action_example.py**
   - Demonstrates how to create custom actions by inheriting from BaseAction
   - Shows how to extend TaskMasterPy's functionality with your own actions

8. **yaml_config_example.py**
   - Shows how to define workflows using YAML configuration files
   - Demonstrates loading workflows from configuration files

## Running the Examples

Before running these examples, make sure you have TaskMasterPy installed:

```bash
pip install taskmasterpy
```

To run any example:

```bash
python examples/simple_taskmaster_example.py
```

Note: Some examples may require sample data files. You can create these in a `data` directory or modify the examples to use your own data files.
