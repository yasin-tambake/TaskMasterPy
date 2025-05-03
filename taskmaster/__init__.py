"""
TaskMasterPy: A Python-based automation framework for data operations.

TaskMasterPy is a framework for building data pipelines and automating
data operations using triggers, actions, and workflows.
"""

__version__ = "0.1.0"

# Import core components
from taskmaster.core.workflow import Workflow
from taskmaster.core.runner import WorkflowRunner

# Import trigger and action base classes
from taskmaster.triggers.base import BaseTrigger
from taskmaster.actions.base import BaseAction

# Import utility functions
from taskmaster.utils.config import load_workflow_from_config, load_workflow_config

# Define what's available for import with "from taskmaster import *"
__all__ = [
    'Workflow',
    'WorkflowRunner',
    'BaseTrigger',
    'BaseAction',
    'load_workflow_from_config',
    'load_workflow_config',
]