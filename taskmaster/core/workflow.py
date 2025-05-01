"""
Workflow module for TaskMasterPy.

This module defines the Workflow class, which represents a complete workflow
consisting of triggers and actions arranged in a directed acyclic graph (DAG).
"""
import uuid
from typing import Dict, Any, List, Optional, Set, Callable
import logging

from taskmaster.triggers.base import BaseTrigger
from taskmaster.actions.base import BaseAction


class Workflow:
    """A workflow is a collection of triggers and actions arranged in a DAG.

    Workflows are the main unit of execution in TaskMasterPy. They consist of
    one or more triggers that initiate the workflow, and a DAG of actions that
    are executed in dependency order.
    """

    def __init__(self, name: str = None, description: str = None):
        """Initialize a new workflow.

        Args:
            name: A unique name for this workflow
            description: A description of what this workflow does
        """
        self.id = str(uuid.uuid4())
        self.name = name or f"Workflow_{self.id[:8]}"
        self.description = description or ""
        self.triggers: List[BaseTrigger] = []
        self.actions: Dict[str, BaseAction] = {}
        self.context: Dict[str, Any] = {}
        self.is_running = False
        self.logger = logging.getLogger(f"taskmaster.workflow.{self.name}")

    def add_trigger(self, trigger: BaseTrigger) -> None:
        """Add a trigger to this workflow.

        Args:
            trigger: The trigger to add
        """
        trigger.register_callback(self._on_trigger_fired)
        self.triggers.append(trigger)
        self.logger.info(f"Added trigger: {trigger}")

    def add_action(self, action: BaseAction) -> None:
        """Add an action to this workflow.

        Args:
            action: The action to add
        """
        self.actions[action.id] = action
        self.logger.info(f"Added action: {action}")

    def add_dependency(self, action: BaseAction, depends_on: BaseAction) -> None:
        """Add a dependency between two actions.

        Args:
            action: The action that depends on another
            depends_on: The action that must complete before the dependent action
        """
        if action.id not in self.actions or depends_on.id not in self.actions:
            raise ValueError("Both actions must be added to the workflow first")

        action.add_dependency(depends_on)
        self.logger.info(f"Added dependency: {action} depends on {depends_on}")

    def _on_trigger_fired(self, trigger: BaseTrigger, event_data: Dict[str, Any]) -> None:
        """Callback function called when a trigger fires.

        Args:
            trigger: The trigger that fired
            event_data: Data associated with the triggering event
        """
        self.logger.info(f"Trigger fired: {trigger}")
        self.run(event_data)

    def get_ready_actions(self) -> List[BaseAction]:
        """Get all actions that are ready to execute.

        Returns:
            A list of actions that have all dependencies satisfied
        """
        # If there are no actions with dependencies, return all pending actions
        if all(len(action.dependencies) == 0 for action in self.actions.values()):
            return [action for action in self.actions.values() if action.status == "pending"]

        return [
            action for action in self.actions.values()
            if action.status == "pending" and action.can_execute()
        ]

    def run(self, event_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """Run the workflow.

        Args:
            event_data: Data associated with the triggering event

        Returns:
            The workflow context after execution
        """
        if self.is_running:
            self.logger.warning("Workflow is already running")
            return self.context

        self.is_running = True
        self.context = {"event_data": event_data or {}}

        try:
            # Reset all actions
            for action in self.actions.values():
                action.status = "pending"
                action.result = None
                action.error = None

            # Execute actions in dependency order
            while True:
                ready_actions = self.get_ready_actions()
                if not ready_actions:
                    # Check if all actions are completed or if some failed
                    if all(a.status in ["completed", "failed"] for a in self.actions.values()):
                        break
                    # If there are no ready actions but some are still pending,
                    # there might be a circular dependency
                    if any(a.status == "pending" for a in self.actions.values()):
                        raise ValueError("Circular dependency detected in workflow")
                    break

                for action in ready_actions:
                    try:
                        self.logger.info(f"Executing action: {action}")
                        result = action.run(self.context)
                        self.context[action.id] = result
                        self.logger.info(f"Action completed: {action}")
                    except Exception as e:
                        self.logger.error(f"Action failed: {action}, error: {str(e)}")
                        # Continue with other actions

            return self.context
        finally:
            self.is_running = False

    def activate(self) -> None:
        """Activate all triggers in this workflow."""
        for trigger in self.triggers:
            trigger.activate()
        self.logger.info("Workflow activated")

    def deactivate(self) -> None:
        """Deactivate all triggers in this workflow."""
        for trigger in self.triggers:
            trigger.deactivate()
        self.logger.info("Workflow deactivated")

    def __str__(self) -> str:
        return f"Workflow(name={self.name}, triggers={len(self.triggers)}, actions={len(self.actions)})"
