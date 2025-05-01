"""
Validation utilities for TaskMasterPy.

This module provides utilities for validating configuration files.
"""
from typing import Dict, Any, List, Tuple, Optional, Union


def validate_workflow_config(config: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Validate a workflow configuration.
    
    Args:
        config: The workflow configuration to validate
        
    Returns:
        A tuple containing a boolean indicating whether the configuration is valid
        and a list of error messages
    """
    errors = []
    
    # Check required fields
    if "name" not in config:
        errors.append("Workflow name is required")
    
    # Validate triggers
    triggers = config.get("triggers", [])
    if not triggers:
        errors.append("At least one trigger is required")
    
    for i, trigger in enumerate(triggers):
        trigger_errors = validate_trigger_config(trigger)
        for error in trigger_errors:
            errors.append(f"Trigger {i+1}: {error}")
    
    # Validate actions
    actions = config.get("actions", [])
    if not actions:
        errors.append("At least one action is required")
    
    action_ids = set()
    for i, action in enumerate(actions):
        action_errors = validate_action_config(action)
        for error in action_errors:
            errors.append(f"Action {i+1}: {error}")
        
        # Check for duplicate action IDs
        action_id = action.get("id")
        if action_id:
            if action_id in action_ids:
                errors.append(f"Duplicate action ID: {action_id}")
            else:
                action_ids.add(action_id)
    
    # Validate dependencies
    for i, action in enumerate(actions):
        dependencies = action.get("depends_on", [])
        for dep_id in dependencies:
            if dep_id not in action_ids:
                errors.append(f"Action {i+1}: Dependency '{dep_id}' not found")
    
    return len(errors) == 0, errors


def validate_trigger_config(config: Dict[str, Any]) -> List[str]:
    """Validate a trigger configuration.
    
    Args:
        config: The trigger configuration to validate
        
    Returns:
        A list of error messages
    """
    errors = []
    
    # Check required fields
    if "type" not in config:
        errors.append("Trigger type is required")
    
    trigger_type = config.get("type", "")
    
    # Validate specific trigger types
    if trigger_type == "time":
        trigger_config = config.get("config", {})
        if "schedule_str" not in trigger_config:
            errors.append("Time trigger requires 'schedule_str' configuration")
    
    elif trigger_type == "cron":
        trigger_config = config.get("config", {})
        if "cron_expression" not in trigger_config:
            errors.append("Cron trigger requires 'cron_expression' configuration")
    
    elif trigger_type == "file":
        trigger_config = config.get("config", {})
        if "path" not in trigger_config:
            errors.append("File trigger requires 'path' configuration")
    
    elif trigger_type == "api_poll":
        trigger_config = config.get("config", {})
        if "url" not in trigger_config:
            errors.append("API poll trigger requires 'url' configuration")
    
    elif trigger_type == "webhook":
        # No specific validation for webhook trigger
        pass
    
    elif trigger_type == "db" or trigger_type == "sqlite":
        trigger_config = config.get("config", {})
        if "connection_string" not in trigger_config:
            errors.append(f"{trigger_type.capitalize()} trigger requires 'connection_string' configuration")
        if "query" not in trigger_config:
            errors.append(f"{trigger_type.capitalize()} trigger requires 'query' configuration")
    
    elif trigger_type:
        errors.append(f"Unsupported trigger type: {trigger_type}")
    
    return errors


def validate_action_config(config: Dict[str, Any]) -> List[str]:
    """Validate an action configuration.
    
    Args:
        config: The action configuration to validate
        
    Returns:
        A list of error messages
    """
    errors = []
    
    # Check required fields
    if "type" not in config:
        errors.append("Action type is required")
    
    action_type = config.get("type", "")
    
    # Validate specific action types
    if action_type.startswith("load_"):
        action_config = config.get("config", {})
        
        if action_type == "load_csv" or action_type == "load_json" or action_type == "load_excel":
            if "file_path" not in action_config:
                errors.append(f"{action_type.capitalize()} action requires 'file_path' configuration")
        
        elif action_type == "load_sql":
            if "connection_string" not in action_config:
                errors.append("LoadSQL action requires 'connection_string' configuration")
            if "query" not in action_config:
                errors.append("LoadSQL action requires 'query' configuration")
    
    elif action_type.startswith("save_"):
        action_config = config.get("config", {})
        
        if action_type == "save_csv" or action_type == "save_json" or action_type == "save_excel":
            if "file_path" not in action_config:
                errors.append(f"{action_type.capitalize()} action requires 'file_path' configuration")
        
        elif action_type == "save_sql":
            if "connection_string" not in action_config:
                errors.append("SaveSQL action requires 'connection_string' configuration")
            if "table_name" not in action_config:
                errors.append("SaveSQL action requires 'table_name' configuration")
    
    elif action_type == "send_email":
        action_config = config.get("config", {})
        required_fields = ["smtp_server", "from_email", "to_email", "subject", "body"]
        for field in required_fields:
            if field not in action_config:
                errors.append(f"SendEmail action requires '{field}' configuration")
    
    elif action_type == "call_api" or action_type == "webhook":
        action_config = config.get("config", {})
        if "url" not in action_config:
            errors.append(f"{action_type.capitalize()} action requires 'url' configuration")
    
    elif action_type == "run_python" or action_type == "run_shell":
        action_config = config.get("config", {})
        if "script" not in action_config and "script_path" not in action_config:
            errors.append(f"{action_type.capitalize()} action requires either 'script' or 'script_path' configuration")
    
    elif action_type == "console_notify" or action_type == "system_notify":
        action_config = config.get("config", {})
        if "message" not in action_config:
            errors.append(f"{action_type.capitalize()} action requires 'message' configuration")
    
    elif action_type and not (
        action_type in ["drop_na", "fix_data_types", "rename_columns", "filter_rows"] or
        action_type in ["normalize", "aggregate", "pivot", "encode"]
    ):
        errors.append(f"Unsupported action type: {action_type}")
    
    return errors
