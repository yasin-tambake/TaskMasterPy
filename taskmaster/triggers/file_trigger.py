"""
File-based triggers for TaskMasterPy.

This module defines triggers that fire based on file system events,
such as file creation, modification, or deletion.
"""
import os
import time
import threading
from typing import Dict, Any, Optional, List, Set
from pathlib import Path

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler, FileSystemEvent
    WATCHDOG_AVAILABLE = True
except ImportError:
    WATCHDOG_AVAILABLE = False


from taskmaster.triggers.base import BaseTrigger


class FileEventHandler(FileSystemEventHandler):
    """Event handler for file system events."""

    def __init__(self, trigger: 'FileTrigger'):
        """Initialize a new file event handler.

        Args:
            trigger: The FileTrigger instance to notify of events
        """
        self.trigger = trigger

    def on_created(self, event: FileSystemEvent) -> None:
        """Called when a file or directory is created.

        Args:
            event: The file system event
        """
        if self.trigger.should_process_event(event, "created"):
            self.trigger.fire({
                "event_type": "created",
                "path": event.src_path,
                "is_directory": event.is_directory,
                "time": time.time()
            })

    def on_modified(self, event: FileSystemEvent) -> None:
        """Called when a file or directory is modified.

        Args:
            event: The file system event
        """
        if self.trigger.should_process_event(event, "modified"):
            self.trigger.fire({
                "event_type": "modified",
                "path": event.src_path,
                "is_directory": event.is_directory,
                "time": time.time()
            })

    def on_deleted(self, event: FileSystemEvent) -> None:
        """Called when a file or directory is deleted.

        Args:
            event: The file system event
        """
        if self.trigger.should_process_event(event, "deleted"):
            self.trigger.fire({
                "event_type": "deleted",
                "path": event.src_path,
                "is_directory": event.is_directory,
                "time": time.time()
            })

    def on_moved(self, event: FileSystemEvent) -> None:
        """Called when a file or directory is moved.

        Args:
            event: The file system event
        """
        if self.trigger.should_process_event(event, "moved"):
            self.trigger.fire({
                "event_type": "moved",
                "src_path": event.src_path,
                "dest_path": event.dest_path,
                "is_directory": event.is_directory,
                "time": time.time()
            })


class FileTrigger(BaseTrigger):
    """A trigger that fires based on file system events.

    This trigger uses the watchdog library to monitor file system events
    and fires when files are created, modified, or deleted.
    """

    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new file trigger.

        Args:
            name: A unique name for this trigger
            config: Configuration parameters for the trigger
                - path: The directory path to watch
                - patterns: List of file patterns to watch (e.g., ["*.csv", "*.txt"])
                - ignore_patterns: List of file patterns to ignore
                - ignore_directories: Whether to ignore directory events
                - event_types: List of event types to watch for
                  (created, modified, deleted, moved)
                - recursive: Whether to watch subdirectories recursively
        """
        if not WATCHDOG_AVAILABLE:
            raise ImportError(
                "watchdog is required for FileTrigger. "
                "Install it with 'pip install watchdog'."
            )

        super().__init__(name, config)
        self.path = self.config.get("path", ".")
        self.patterns = self.config.get("patterns", ["*"])
        self.ignore_patterns = self.config.get("ignore_patterns", [])
        self.ignore_directories = self.config.get("ignore_directories", False)
        self.event_types = self.config.get("event_types", ["created", "modified", "deleted", "moved"])
        self.recursive = self.config.get("recursive", True)

        self.observer: Optional[Observer] = None
        self.event_handler: Optional[FileEventHandler] = None
        self.processed_events: Set[str] = set()
        self.last_event_time = 0
        self.debounce_interval = self.config.get("debounce_interval", 0.5)  # seconds

    def activate(self) -> None:
        """Activate the trigger to start watching for file events."""
        super().activate()

        # Create the event handler and observer
        self.event_handler = FileEventHandler(self)
        self.observer = Observer()

        # Schedule the directory for watching
        self.observer.schedule(
            self.event_handler,
            self.path,
            recursive=self.recursive
        )

        # Start the observer
        self.observer.start()

    def deactivate(self) -> None:
        """Deactivate the trigger to stop watching for file events."""
        if self.observer:
            self.observer.stop()
            self.observer.join()
            self.observer = None

        self.is_active = False

    def should_process_event(self, event: FileSystemEvent, event_type: str) -> bool:
        """Check if an event should be processed.

        Args:
            event: The file system event
            event_type: The type of event (created, modified, deleted, moved)

        Returns:
            True if the event should be processed, False otherwise
        """
        # Check if the event type is in the list of event types to watch for
        if event_type not in self.event_types:
            return False

        # Check if the event is for a directory and we're ignoring directories
        if event.is_directory and self.ignore_directories:
            return False

        # Get the path as a string
        path = event.src_path

        # Check if the path matches any of the patterns
        path_obj = Path(path)
        filename = path_obj.name

        # Check if the file matches any of the ignore patterns
        for pattern in self.ignore_patterns:
            if path_obj.match(pattern):
                return False

        # Check if the file matches any of the include patterns
        matches_pattern = False
        for pattern in self.patterns:
            if path_obj.match(pattern):
                matches_pattern = True
                break

        if not matches_pattern:
            return False

        # Debounce events to avoid duplicates
        current_time = time.time()
        event_key = f"{event_type}:{path}:{current_time:.1f}"

        if event_key in self.processed_events:
            return False

        # If the event is too close to the last event, ignore it
        if current_time - self.last_event_time < self.debounce_interval:
            return False

        # Add the event to the processed events set and update the last event time
        self.processed_events.add(event_key)
        self.last_event_time = current_time

        # Clean up old processed events
        self._clean_processed_events()

        return True

    def _clean_processed_events(self) -> None:
        """Clean up old processed events to prevent memory leaks."""
        current_time = time.time()
        # Keep only events from the last minute
        self.processed_events = {
            event for event in self.processed_events
            if float(event.split(":")[-1]) > current_time - 60
        }