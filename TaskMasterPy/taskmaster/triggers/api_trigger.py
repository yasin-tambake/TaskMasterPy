"""
API-based triggers for TaskMasterPy.

This module defines triggers that fire based on API polling or webhooks.
"""
import threading
import time
import json
import hashlib
from typing import Dict, Any, Optional, List, Callable
import requests

from taskmaster.triggers.base import BaseTrigger


class APIPollTrigger(BaseTrigger):
    """A trigger that fires based on polling an API endpoint.
    
    This trigger periodically polls an API endpoint and fires when
    the response changes or meets certain criteria.
    """
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new API poll trigger.
        
        Args:
            name: A unique name for this trigger
            config: Configuration parameters for the trigger
                - url: The URL to poll
                - method: The HTTP method to use (GET, POST, etc.)
                - headers: HTTP headers to include in the request
                - data: Data to include in the request body
                - interval: How often to poll the API (in seconds)
                - response_type: How to parse the response (json, text, binary)
                - trigger_condition: When to fire the trigger
                  (any_change, specific_value, jmespath)
                - condition_value: The value to compare against for specific_value
                - jmespath_expression: The JMESPath expression to evaluate
        """
        super().__init__(name, config)
        self.url = self.config.get("url", "")
        self.method = self.config.get("method", "GET")
        self.headers = self.config.get("headers", {})
        self.data = self.config.get("data", None)
        self.interval = self.config.get("interval", 60)  # seconds
        self.response_type = self.config.get("response_type", "json")
        self.trigger_condition = self.config.get("trigger_condition", "any_change")
        self.condition_value = self.config.get("condition_value", None)
        self.jmespath_expression = self.config.get("jmespath_expression", None)
        
        self.thread: Optional[threading.Thread] = None
        self.last_response: Any = None
        self.last_response_hash: Optional[str] = None
    
    def activate(self) -> None:
        """Activate the trigger to start polling the API."""
        super().activate()
        
        # Start the polling thread
        self.thread = threading.Thread(target=self._poll_api, daemon=True)
        self.thread.start()
    
    def deactivate(self) -> None:
        """Deactivate the trigger to stop polling the API."""
        self.is_active = False
        if self.thread:
            self.thread.join(timeout=1.0)
            self.thread = None
    
    def _poll_api(self) -> None:
        """Poll the API endpoint periodically."""
        while self.is_active:
            try:
                # Make the API request
                response = requests.request(
                    method=self.method,
                    url=self.url,
                    headers=self.headers,
                    data=self.data,
                    timeout=30
                )
                
                # Parse the response based on the specified type
                if self.response_type == "json":
                    response_data = response.json()
                elif self.response_type == "text":
                    response_data = response.text
                else:  # binary
                    response_data = response.content
                
                # Check if we should fire the trigger
                if self._should_fire(response_data):
                    self.fire({
                        "url": self.url,
                        "response": response_data,
                        "status_code": response.status_code,
                        "time": time.time()
                    })
                
                # Update the last response
                self.last_response = response_data
                self.last_response_hash = self._hash_response(response_data)
                
            except Exception as e:
                # Log the error but continue polling
                print(f"Error polling API {self.url}: {str(e)}")
            
            # Sleep until the next poll
            time.sleep(self.interval)
    
    def _should_fire(self, response_data: Any) -> bool:
        """Check if the trigger should fire based on the response.
        
        Args:
            response_data: The parsed response data
            
        Returns:
            True if the trigger should fire, False otherwise
        """
        if self.trigger_condition == "any_change":
            # Fire if the response has changed
            current_hash = self._hash_response(response_data)
            return self.last_response_hash is not None and current_hash != self.last_response_hash
        
        elif self.trigger_condition == "specific_value":
            # Fire if the response equals a specific value
            return response_data == self.condition_value
        
        elif self.trigger_condition == "jmespath":
            # Fire if the JMESPath expression evaluates to a truthy value
            if self.jmespath_expression:
                try:
                    import jmespath
                    result = jmespath.search(self.jmespath_expression, response_data)
                    return bool(result)
                except ImportError:
                    print("jmespath library not installed, falling back to any_change")
                    return self._should_fire_any_change(response_data)
                except Exception as e:
                    print(f"Error evaluating JMESPath expression: {str(e)}")
                    return False
            return False
        
        return False
    
    def _hash_response(self, response_data: Any) -> str:
        """Create a hash of the response data for comparison.
        
        Args:
            response_data: The response data to hash
            
        Returns:
            A hash of the response data
        """
        if isinstance(response_data, (dict, list)):
            # For structured data, hash the JSON string
            data_str = json.dumps(response_data, sort_keys=True)
        elif isinstance(response_data, bytes):
            # For binary data, hash the bytes directly
            return hashlib.md5(response_data).hexdigest()
        else:
            # For text, hash the string
            data_str = str(response_data)
        
        return hashlib.md5(data_str.encode("utf-8")).hexdigest()
