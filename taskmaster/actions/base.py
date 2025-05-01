"""
Base Action class for TaskMasterPy.

This module defines the base class for all actions in the TaskMasterPy framework.
Actions are the building blocks of workflows and perform specific tasks.
"""
from abc import ABC, abstractmethod
import uuid
from typing import Dict, Any, Optional, List


class BaseAction(ABC):
    """Base class for all actions in TaskMasterPy.
    
    An action is a unit of work that can be executed as part of a workflow.
    All action types should inherit from this class and implement the required methods.
    """
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new action.
        
        Args:
            name: A unique name for this action instance
            config: Configuration parameters for the action
        """
        self.id = str(uuid.uuid4())
        self.name = name or f"{self.__class__.__name__}_{self.id[:8]}"
        self.config = config or {}
        self.dependencies: List['BaseAction'] = []
        self.result: Any = None
        self.status = "pending"  # pending, running, completed, failed
        self.error: Optional[Exception] = None
    
    def add_dependency(self, action: 'BaseAction') -> None:
        """Add a dependency to this action.
        
        Args:
            action: Another action that must complete before this one can run
        """
        self.dependencies.append(action)
    
    def can_execute(self) -> bool:
        """Check if this action can be executed.
        
        Returns:
            True if all dependencies have completed successfully, False otherwise
        """
        return all(dep.status == "completed" for dep in self.dependencies)
    
    @abstractmethod
    def execute(self, context: Dict[str, Any] = None) -> Any:
        """Execute the action.
        
        Args:
            context: Execution context, including results from previous actions
            
        Returns:
            The result of the action execution
        """
        pass
    
    def run(self, context: Dict[str, Any] = None) -> Any:
        """Run the action, updating its status and handling errors.
        
        Args:
            context: Execution context, including results from previous actions
            
        Returns:
            The result of the action execution
        """
        context = context or {}
        self.status = "running"
        try:
            self.result = self.execute(context)
            self.status = "completed"
            return self.result
        except Exception as e:
            self.status = "failed"
            self.error = e
            raise
    
    def __str__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name}, status={self.status})"
