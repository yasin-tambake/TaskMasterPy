"""
Database-based triggers for TaskMasterPy.

This module defines triggers that fire based on database events,
such as inserts, updates, or deletes.
"""
import threading
import time
import json
import hashlib
from typing import Dict, Any, Optional, List, Callable, Union
import sqlite3

from taskmaster.triggers.base import BaseTrigger


class DBTrigger(BaseTrigger):
    """A trigger that fires based on database events.
    
    This trigger periodically polls a database table and fires when
    the data changes or meets certain criteria.
    """
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new database trigger.
        
        Args:
            name: A unique name for this trigger
            config: Configuration parameters for the trigger
                - connection_string: Database connection string
                - query: SQL query to execute
                - interval: How often to poll the database (in seconds)
                - trigger_condition: When to fire the trigger
                  (any_change, row_count_change, specific_value)
                - condition_value: The value to compare against for specific_value
        """
        super().__init__(name, config)
        self.connection_string = self.config.get("connection_string", "")
        self.query = self.config.get("query", "")
        self.interval = self.config.get("interval", 60)  # seconds
        self.trigger_condition = self.config.get("trigger_condition", "any_change")
        self.condition_value = self.config.get("condition_value", None)
        
        self.thread: Optional[threading.Thread] = None
        self.last_result: Any = None
        self.last_result_hash: Optional[str] = None
        self.last_row_count: int = 0
    
    def activate(self) -> None:
        """Activate the trigger to start polling the database."""
        super().activate()
        
        # Start the polling thread
        self.thread = threading.Thread(target=self._poll_database, daemon=True)
        self.thread.start()
    
    def deactivate(self) -> None:
        """Deactivate the trigger to stop polling the database."""
        self.is_active = False
        if self.thread:
            self.thread.join(timeout=1.0)
            self.thread = None
    
    def _poll_database(self) -> None:
        """Poll the database periodically."""
        while self.is_active:
            try:
                # Execute the query
                result = self._execute_query()
                
                # Check if we should fire the trigger
                if self._should_fire(result):
                    self.fire({
                        "query": self.query,
                        "result": result,
                        "row_count": len(result) if isinstance(result, list) else 1,
                        "time": time.time()
                    })
                
                # Update the last result
                self.last_result = result
                self.last_result_hash = self._hash_result(result)
                self.last_row_count = len(result) if isinstance(result, list) else 1
                
            except Exception as e:
                # Log the error but continue polling
                print(f"Error polling database: {str(e)}")
            
            # Sleep until the next poll
            time.sleep(self.interval)
    
    def _execute_query(self) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """Execute the SQL query.
        
        Returns:
            The query result as a list of dictionaries
        """
        # For simplicity, we'll only support SQLite for now
        # A more complete implementation would support multiple database types
        
        conn = sqlite3.connect(self.connection_string)
        conn.row_factory = sqlite3.Row
        
        cursor = conn.cursor()
        cursor.execute(self.query)
        
        # Convert the result to a list of dictionaries
        result = [dict(row) for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return result
    
    def _should_fire(self, result: Union[List[Dict[str, Any]], Dict[str, Any]]) -> bool:
        """Check if the trigger should fire based on the query result.
        
        Args:
            result: The query result
            
        Returns:
            True if the trigger should fire, False otherwise
        """
        if self.trigger_condition == "any_change":
            # Fire if the result has changed
            current_hash = self._hash_result(result)
            return self.last_result_hash is not None and current_hash != self.last_result_hash
        
        elif self.trigger_condition == "row_count_change":
            # Fire if the number of rows has changed
            current_row_count = len(result) if isinstance(result, list) else 1
            return self.last_row_count != current_row_count
        
        elif self.trigger_condition == "specific_value":
            # Fire if the result equals a specific value
            return result == self.condition_value
        
        return False
    
    def _hash_result(self, result: Union[List[Dict[str, Any]], Dict[str, Any]]) -> str:
        """Create a hash of the query result for comparison.
        
        Args:
            result: The query result to hash
            
        Returns:
            A hash of the query result
        """
        # Convert the result to a JSON string and hash it
        result_str = json.dumps(result, sort_keys=True)
        return hashlib.md5(result_str.encode("utf-8")).hexdigest()


class SQLiteDBTrigger(DBTrigger):
    """A trigger specifically for SQLite databases."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new SQLite database trigger.
        
        Args:
            name: A unique name for this trigger
            config: Configuration parameters for the trigger
                - database_path: Path to the SQLite database file
                - query: SQL query to execute
                - interval: How often to poll the database (in seconds)
                - trigger_condition: When to fire the trigger
                  (any_change, row_count_change, specific_value)
                - condition_value: The value to compare against for specific_value
        """
        config = config or {}
        
        # Convert database_path to connection_string
        if "database_path" in config and "connection_string" not in config:
            config["connection_string"] = config["database_path"]
        
        super().__init__(name, config)
