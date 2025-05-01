"""
Base Trigger class for TaskMasterPy.

This module defines the base class for all triggers in the TaskMasterPy framework.
Triggers are responsible for initiating workflows based on specific events.
"""
from abc import ABC, abstractmethod
import uuid
from typing import Dict, Any, Optional, List, Callable


class BaseTrigger(ABC):
    """Base class for all triggers in TaskMasterPy.
    
    A trigger is responsible for detecting events and initiating workflows.
    All trigger types should inherit from this class and implement the required methods.
    """
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new trigger.
        
        Args:
            name: A unique name for this trigger instance
            config: Configuration parameters for the trigger
        """
        self.id = str(uuid.uuid4())
        self.name = name or f"{self.__class__.__name__}_{self.id[:8]}"
        self.config = config or {}
        self.callbacks: List[Callable] = []
        self.is_active = False
    
    def register_callback(self, callback: Callable) -> None:
        """Register a callback function to be called when the trigger fires.
        
        Args:
            callback: A function to call when the trigger fires
        """
        self.callbacks.append(callback)
    
    def fire(self, event_data: Dict[str, Any] = None) -> None:
        """Fire the trigger and call all registered callbacks.
        
        Args:
            event_data: Data associated with the triggering event
        """
        event_data = event_data or {}
        for callback in self.callbacks:
            callback(self, event_data)
    
    @abstractmethod
    def activate(self) -> None:
        """Activate the trigger to start listening for events."""
        self.is_active = True
    
    @abstractmethod
    def deactivate(self) -> None:
        """Deactivate the trigger to stop listening for events."""
        self.is_active = False
    
    def __str__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name})"
