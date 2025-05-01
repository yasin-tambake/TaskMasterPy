"""
Configuration utilities for TaskMasterPy.

This module provides utilities for loading and parsing configuration files.
"""
import os
import yaml
import json
from typing import Dict, Any, List, Tuple, Optional, Union

from taskmaster.core.workflow import Workflow
from taskmaster.triggers.base import BaseTrigger
from taskmaster.actions.base import BaseAction
from taskmaster.utils.validators import validate_workflow_config

# Import all trigger types
from taskmaster.triggers.time_trigger import TimeTrigger, CronTrigger
from taskmaster.triggers.file_trigger import FileTrigger
from taskmaster.triggers.api_trigger import APIPollTrigger
from taskmaster.triggers.webhook_trigger import WebhookTrigger
from taskmaster.triggers.db_trigger import DBTrigger, SQLiteDBTrigger

# Import all action types
from taskmaster.actions.load_data import LoadCSVAction, LoadJSONAction, LoadExcelAction, LoadSQLAction
from taskmaster.actions.clean_data import DropNAAction, FixDataTypesAction, RenameColumnsAction, FilterRowsAction
from taskmaster.actions.transform_data import NormalizeAction, AggregateAction, PivotAction, EncodeAction
from taskmaster.actions.save_data import SaveCSVAction, SaveJSONAction, SaveExcelAction, SaveSQLAction
from taskmaster.actions.email import SendEmailAction
from taskmaster.actions.api import CallAPIAction, WebhookAction
from taskmaster.actions.script import RunPythonScriptAction, RunShellScriptAction
from taskmaster.actions.notify import ConsoleNotifyAction, SystemNotifyAction


def load_workflow_config(config_path: str) -> Dict[str, Any]:
    """Load a workflow configuration from a file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        The workflow configuration as a dictionary
    """
    with open(config_path, "r") as f:
        if config_path.endswith((".yaml", ".yml")):
            return yaml.safe_load(f)
        elif config_path.endswith(".json"):
            return json.load(f)
        else:
            raise ValueError(f"Unsupported file format: {config_path}")


def load_workflow_from_config(config: Dict[str, Any]) -> Workflow:
    """Create a workflow from a configuration dictionary.
    
    Args:
        config: The workflow configuration
        
    Returns:
        The created workflow
    """
    # Validate the configuration
    is_valid, errors = validate_workflow_config(config)
    if not is_valid:
        raise ValueError(f"Invalid workflow configuration: {errors}")
    
    # Create the workflow
    workflow = Workflow(
        name=config.get("name", ""),
        description=config.get("description", "")
    )
    
    # Create and add triggers
    triggers_config = config.get("triggers", [])
    for trigger_config in triggers_config:
        trigger = create_trigger_from_config(trigger_config)
        workflow.add_trigger(trigger)
    
    # Create and add actions
    actions_config = config.get("actions", [])
    action_map = {}
    
    for action_config in actions_config:
        action = create_action_from_config(action_config)
        workflow.add_action(action)
        action_map[action_config.get("id", action.id)] = action
    
    # Add dependencies
    for action_config in actions_config:
        action_id = action_config.get("id", "")
        dependencies = action_config.get("depends_on", [])
        
        if action_id and dependencies:
            action = action_map.get(action_id)
            if action:
                for dep_id in dependencies:
                    dep_action = action_map.get(dep_id)
                    if dep_action:
                        workflow.add_dependency(action, dep_action)
    
    return workflow


def create_trigger_from_config(config: Dict[str, Any]) -> BaseTrigger:
    """Create a trigger from a configuration dictionary.
    
    Args:
        config: The trigger configuration
        
    Returns:
        The created trigger
    """
    trigger_type = config.get("type", "")
    name = config.get("name", "")
    trigger_config = config.get("config", {})
    
    # Create the appropriate trigger type
    if trigger_type == "time":
        return TimeTrigger(name, trigger_config)
    elif trigger_type == "cron":
        return CronTrigger(name, trigger_config)
    elif trigger_type == "file":
        return FileTrigger(name, trigger_config)
    elif trigger_type == "api_poll":
        return APIPollTrigger(name, trigger_config)
    elif trigger_type == "webhook":
        return WebhookTrigger(name, trigger_config)
    elif trigger_type == "db":
        return DBTrigger(name, trigger_config)
    elif trigger_type == "sqlite":
        return SQLiteDBTrigger(name, trigger_config)
    else:
        raise ValueError(f"Unsupported trigger type: {trigger_type}")


def create_action_from_config(config: Dict[str, Any]) -> BaseAction:
    """Create an action from a configuration dictionary.
    
    Args:
        config: The action configuration
        
    Returns:
        The created action
    """
    action_type = config.get("type", "")
    name = config.get("name", "")
    action_config = config.get("config", {})
    
    # Create the appropriate action type
    if action_type == "load_csv":
        return LoadCSVAction(name, action_config)
    elif action_type == "load_json":
        return LoadJSONAction(name, action_config)
    elif action_type == "load_excel":
        return LoadExcelAction(name, action_config)
    elif action_type == "load_sql":
        return LoadSQLAction(name, action_config)
    elif action_type == "drop_na":
        return DropNAAction(name, action_config)
    elif action_type == "fix_data_types":
        return FixDataTypesAction(name, action_config)
    elif action_type == "rename_columns":
        return RenameColumnsAction(name, action_config)
    elif action_type == "filter_rows":
        return FilterRowsAction(name, action_config)
    elif action_type == "normalize":
        return NormalizeAction(name, action_config)
    elif action_type == "aggregate":
        return AggregateAction(name, action_config)
    elif action_type == "pivot":
        return PivotAction(name, action_config)
    elif action_type == "encode":
        return EncodeAction(name, action_config)
    elif action_type == "save_csv":
        return SaveCSVAction(name, action_config)
    elif action_type == "save_json":
        return SaveJSONAction(name, action_config)
    elif action_type == "save_excel":
        return SaveExcelAction(name, action_config)
    elif action_type == "save_sql":
        return SaveSQLAction(name, action_config)
    elif action_type == "send_email":
        return SendEmailAction(name, action_config)
    elif action_type == "call_api":
        return CallAPIAction(name, action_config)
    elif action_type == "webhook":
        return WebhookAction(name, action_config)
    elif action_type == "run_python":
        return RunPythonScriptAction(name, action_config)
    elif action_type == "run_shell":
        return RunShellScriptAction(name, action_config)
    elif action_type == "console_notify":
        return ConsoleNotifyAction(name, action_config)
    elif action_type == "system_notify":
        return SystemNotifyAction(name, action_config)
    else:
        raise ValueError(f"Unsupported action type: {action_type}")
