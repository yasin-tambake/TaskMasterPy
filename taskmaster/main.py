"""
Main entry point for TaskMasterPy.

This module provides the main entry point for the TaskMasterPy CLI.
"""
import os
import sys
from typing import Dict, Any, Optional, List, Union

from taskmaster.cli.commands import app
from taskmaster.utils.logging import configure_logging
from taskmaster.plugins.loader import load_plugins, load_plugins_from_entry_points


def main():
    """Main entry point for TaskMasterPy."""
    # Configure logging
    configure_logging()
    
    # Load plugins
    plugin_dir = os.path.join(os.path.dirname(__file__), "plugins")
    load_plugins(plugin_dir)
    
    # Load plugins from entry points
    load_plugins_from_entry_points()
    
    # Run the CLI app
    app()


if __name__ == "__main__":
    main()
