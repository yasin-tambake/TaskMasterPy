"""
Triggers package for TaskMasterPy.

This package contains all the trigger classes for TaskMasterPy.
"""

from taskmaster.triggers.base import BaseTrigger
from taskmaster.triggers.time_trigger import TimeTrigger, CronTrigger
from taskmaster.triggers.file_trigger import FileTrigger
from taskmaster.triggers.api_trigger import APIPollTrigger
from taskmaster.triggers.webhook_trigger import WebhookTrigger
from taskmaster.triggers.db_trigger import DBTrigger, SQLiteDBTrigger

# Define what's available for import with "from taskmaster.triggers import *"
__all__ = [
    'BaseTrigger',
    'TimeTrigger',
    'CronTrigger',
    'FileTrigger',
    'APIPollTrigger',
    'WebhookTrigger',
    'DBTrigger',
    'SQLiteDBTrigger',
]