"""
Runner module for TaskMasterPy.

This module defines the WorkflowRunner class, which is responsible for
executing workflows either immediately or on a schedule.
"""
import logging
from typing import Dict, Any, List, Optional

from taskmaster.core.workflow import Workflow


class WorkflowRunner:
    """Runner for executing workflows.
    
    The WorkflowRunner is responsible for executing workflows, either
    immediately or on a schedule.
    """
    
    def __init__(self):
        """Initialize a new workflow runner."""
        self.workflows: Dict[str, Workflow] = {}
        self.active_workflows: Dict[str, Workflow] = {}
        self.logger = logging.getLogger("taskmaster.runner")
    
    def register_workflow(self, workflow: Workflow) -> None:
        """Register a workflow with this runner.
        
        Args:
            workflow: The workflow to register
        """
        self.workflows[workflow.id] = workflow
        self.logger.info(f"Registered workflow: {workflow}")
    
    def unregister_workflow(self, workflow_id: str) -> None:
        """Unregister a workflow from this runner.
        
        Args:
            workflow_id: The ID of the workflow to unregister
        """
        if workflow_id in self.active_workflows:
            self.stop_workflow(workflow_id)
        
        if workflow_id in self.workflows:
            workflow = self.workflows.pop(workflow_id)
            self.logger.info(f"Unregistered workflow: {workflow}")
    
    def start_workflow(self, workflow_id: str) -> None:
        """Start a workflow.
        
        This activates all triggers in the workflow, allowing it to be
        executed when the triggers fire.
        
        Args:
            workflow_id: The ID of the workflow to start
        """
        if workflow_id not in self.workflows:
            raise ValueError(f"Workflow with ID {workflow_id} not found")
        
        workflow = self.workflows[workflow_id]
        workflow.activate()
        self.active_workflows[workflow_id] = workflow
        self.logger.info(f"Started workflow: {workflow}")
    
    def stop_workflow(self, workflow_id: str) -> None:
        """Stop a workflow.
        
        This deactivates all triggers in the workflow, preventing it from
        being executed when the triggers would otherwise fire.
        
        Args:
            workflow_id: The ID of the workflow to stop
        """
        if workflow_id not in self.active_workflows:
            self.logger.warning(f"Workflow with ID {workflow_id} is not active")
            return
        
        workflow = self.active_workflows.pop(workflow_id)
        workflow.deactivate()
        self.logger.info(f"Stopped workflow: {workflow}")
    
    def run_workflow_now(self, workflow_id: str, event_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """Run a workflow immediately.
        
        This executes the workflow immediately, regardless of its triggers.
        
        Args:
            workflow_id: The ID of the workflow to run
            event_data: Data to pass to the workflow
            
        Returns:
            The workflow context after execution
        """
        if workflow_id not in self.workflows:
            raise ValueError(f"Workflow with ID {workflow_id} not found")
        
        workflow = self.workflows[workflow_id]
        self.logger.info(f"Running workflow now: {workflow}")
        return workflow.run(event_data)
    
    def start_all_workflows(self) -> None:
        """Start all registered workflows."""
        for workflow_id in self.workflows:
            if workflow_id not in self.active_workflows:
                self.start_workflow(workflow_id)
    
    def stop_all_workflows(self) -> None:
        """Stop all active workflows."""
        for workflow_id in list(self.active_workflows.keys()):
            self.stop_workflow(workflow_id)
    
    def get_workflow_status(self, workflow_id: str) -> Dict[str, Any]:
        """Get the status of a workflow.
        
        Args:
            workflow_id: The ID of the workflow to get status for
            
        Returns:
            A dictionary containing the workflow status
        """
        if workflow_id not in self.workflows:
            raise ValueError(f"Workflow with ID {workflow_id} not found")
        
        workflow = self.workflows[workflow_id]
        is_active = workflow_id in self.active_workflows
        
        action_statuses = {
            action_id: {
                "name": action.name,
                "status": action.status,
                "has_error": action.error is not None,
                "error_message": str(action.error) if action.error else None
            }
            for action_id, action in workflow.actions.items()
        }
        
        return {
            "id": workflow.id,
            "name": workflow.name,
            "description": workflow.description,
            "is_active": is_active,
            "is_running": workflow.is_running,
            "trigger_count": len(workflow.triggers),
            "action_count": len(workflow.actions),
            "actions": action_statuses
        }
