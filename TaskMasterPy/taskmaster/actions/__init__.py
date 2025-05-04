"""
Actions package for TaskMasterPy.

This package contains all the action classes for TaskMasterPy.
"""

from taskmaster.actions.base import BaseAction
from taskmaster.actions.load_data import (
    LoadDataAction, LoadCSVAction, LoadJSONAction, LoadExcelAction, LoadSQLAction
)
from taskmaster.actions.clean_data import (
    CleanDataAction, DropNAAction, FixDataTypesAction, RenameColumnsAction, FilterRowsAction
)
from taskmaster.actions.transform_data import (
    TransformDataAction, NormalizeAction, AggregateAction, PivotAction, EncodeAction
)
from taskmaster.actions.save_data import (
    SaveDataAction, SaveCSVAction, SaveJSONAction, SaveExcelAction, SaveSQLAction
)
from taskmaster.actions.api import CallAPIAction, WebhookAction
from taskmaster.actions.script import RunScriptAction, RunPythonScriptAction, RunShellScriptAction
from taskmaster.actions.notify import ConsoleNotifyAction, SystemNotifyAction