"""
Plugin loader for TaskMasterPy.

This module provides utilities for loading plugins.
"""
import os
import sys
import importlib
import inspect
from typing import Dict, Any, List, Tuple, Optional, Union, Type

from taskmaster.triggers.base import BaseTrigger
from taskmaster.actions.base import BaseAction


class PluginRegistry:
    """Registry for TaskMasterPy plugins."""
    
    _instance = None
    
    def __new__(cls):
        """Create a singleton instance."""
        if cls._instance is None:
            cls._instance = super(PluginRegistry, cls).__new__(cls)
            cls._instance._triggers = {}
            cls._instance._actions = {}
        return cls._instance
    
    def register_trigger(self, name: str, trigger_class: Type[BaseTrigger]) -> None:
        """Register a trigger plugin.
        
        Args:
            name: The name of the trigger
            trigger_class: The trigger class
        """
        if not issubclass(trigger_class, BaseTrigger):
            raise TypeError(f"Trigger class must inherit from BaseTrigger: {trigger_class}")
        
        self._triggers[name] = trigger_class
    
    def register_action(self, name: str, action_class: Type[BaseAction]) -> None:
        """Register an action plugin.
        
        Args:
            name: The name of the action
            action_class: The action class
        """
        if not issubclass(action_class, BaseAction):
            raise TypeError(f"Action class must inherit from BaseAction: {action_class}")
        
        self._actions[name] = action_class
    
    def get_trigger(self, name: str) -> Optional[Type[BaseTrigger]]:
        """Get a registered trigger plugin.
        
        Args:
            name: The name of the trigger
            
        Returns:
            The trigger class, or None if not found
        """
        return self._triggers.get(name)
    
    def get_action(self, name: str) -> Optional[Type[BaseAction]]:
        """Get a registered action plugin.
        
        Args:
            name: The name of the action
            
        Returns:
            The action class, or None if not found
        """
        return self._actions.get(name)
    
    def get_all_triggers(self) -> Dict[str, Type[BaseTrigger]]:
        """Get all registered trigger plugins.
        
        Returns:
            A dictionary mapping trigger names to trigger classes
        """
        return self._triggers.copy()
    
    def get_all_actions(self) -> Dict[str, Type[BaseAction]]:
        """Get all registered action plugins.
        
        Returns:
            A dictionary mapping action names to action classes
        """
        return self._actions.copy()


def register_trigger(name: str):
    """Decorator to register a trigger plugin.
    
    Args:
        name: The name of the trigger
        
    Returns:
        A decorator function
    """
    def decorator(cls):
        registry = PluginRegistry()
        registry.register_trigger(name, cls)
        return cls
    return decorator


def register_action(name: str):
    """Decorator to register an action plugin.
    
    Args:
        name: The name of the action
        
    Returns:
        A decorator function
    """
    def decorator(cls):
        registry = PluginRegistry()
        registry.register_action(name, cls)
        return cls
    return decorator


def load_plugins(plugin_dir: str) -> None:
    """Load plugins from a directory.
    
    Args:
        plugin_dir: Path to the plugin directory
    """
    # Check if the directory exists
    if not os.path.exists(plugin_dir):
        return
    
    # Add the plugin directory to the Python path
    sys.path.insert(0, plugin_dir)
    
    # Load all Python files in the directory
    for file in os.listdir(plugin_dir):
        if file.endswith(".py") and not file.startswith("__"):
            module_name = file[:-3]  # Remove .py extension
            try:
                importlib.import_module(module_name)
            except Exception as e:
                print(f"Error loading plugin {module_name}: {str(e)}")
    
    # Remove the plugin directory from the Python path
    sys.path.pop(0)


def load_plugins_from_entry_points() -> None:
    """Load plugins from entry points."""
    try:
        import pkg_resources
    except ImportError:
        return
    
    # Load trigger plugins
    for entry_point in pkg_resources.iter_entry_points("taskmaster.triggers"):
        try:
            trigger_class = entry_point.load()
            registry = PluginRegistry()
            registry.register_trigger(entry_point.name, trigger_class)
        except Exception as e:
            print(f"Error loading trigger plugin {entry_point.name}: {str(e)}")
    
    # Load action plugins
    for entry_point in pkg_resources.iter_entry_points("taskmaster.actions"):
        try:
            action_class = entry_point.load()
            registry = PluginRegistry()
            registry.register_action(entry_point.name, action_class)
        except Exception as e:
            print(f"Error loading action plugin {entry_point.name}: {str(e)}")
