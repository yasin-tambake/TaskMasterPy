"""
Plugins package for TaskMasterPy.

This package contains the plugin system for TaskMasterPy, allowing
for extension of the framework with custom triggers and actions.
"""

from taskmaster.plugins.loader import load_plugins, load_plugins_from_entry_points

__all__ = [
    'load_plugins',
    'load_plugins_from_entry_points',
]