"""
API-related actions for TaskMasterPy.

This module defines actions for interacting with APIs, such as making
HTTP requests or sending webhooks.
"""
import json
import requests
from typing import Dict, Any, Optional, List, Union
import pandas as pd

from taskmaster.actions.base import BaseAction


class CallAPIAction(BaseAction):
    """Action to make HTTP requests to an API endpoint."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new call API action.
        
        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
                - url: The URL to call
                - method: The HTTP method to use (default: 'GET')
                - headers: HTTP headers to include in the request
                - params: Query parameters to include in the request
                - data: Data to include in the request body
                - json: JSON data to include in the request body
                - auth: Authentication credentials (username, password)
                - timeout: Request timeout in seconds (default: 30)
                - verify: Whether to verify SSL certificates (default: True)
                - return_type: How to parse the response (default: 'json')
                  Options: 'json', 'text', 'binary', 'dataframe'
        """
        super().__init__(name, config)
    
    def execute(self, context: Dict[str, Any] = None) -> Any:
        """Execute the action to make an API call.
        
        Args:
            context: Execution context
            
        Returns:
            The API response, parsed according to return_type
        """
        context = context or {}
        
        # Get parameters from config
        url = self.config.get("url", "")
        method = self.config.get("method", "GET")
        headers = self.config.get("headers", {})
        params = self.config.get("params", {})
        data = self.config.get("data")
        json_data = self.config.get("json")
        auth = self.config.get("auth")
        timeout = self.config.get("timeout", 30)
        verify = self.config.get("verify", True)
        return_type = self.config.get("return_type", "json")
        
        # Check if URL is provided
        if not url:
            raise ValueError("URL is required")
        
        # Make the API request
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            data=data,
            json=json_data,
            auth=auth,
            timeout=timeout,
            verify=verify
        )
        
        # Check if the request was successful
        response.raise_for_status()
        
        # Parse the response based on the specified return type
        if return_type == "json":
            return response.json()
        elif return_type == "text":
            return response.text
        elif return_type == "binary":
            return response.content
        elif return_type == "dataframe":
            # Try to parse the response as JSON and convert to DataFrame
            try:
                json_data = response.json()
                if isinstance(json_data, list):
                    return pd.DataFrame(json_data)
                elif isinstance(json_data, dict):
                    # If it's a dict with a data key that's a list, use that
                    if "data" in json_data and isinstance(json_data["data"], list):
                        return pd.DataFrame(json_data["data"])
                    # Otherwise, try to convert the dict to a DataFrame
                    return pd.DataFrame([json_data])
                else:
                    raise ValueError(f"Cannot convert response to DataFrame: {type(json_data)}")
            except Exception as e:
                raise ValueError(f"Error converting response to DataFrame: {str(e)}")
        else:
            raise ValueError(f"Unknown return type: {return_type}")


class WebhookAction(BaseAction):
    """Action to send a webhook to an external service."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new webhook action.
        
        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
                - url: The webhook URL
                - method: The HTTP method to use (default: 'POST')
                - headers: HTTP headers to include in the request
                - payload: Data to include in the request body
                - timeout: Request timeout in seconds (default: 30)
        """
        super().__init__(name, config)
    
    def execute(self, context: Dict[str, Any] = None) -> Dict[str, Any]:
        """Execute the action to send a webhook.
        
        Args:
            context: Execution context
            
        Returns:
            A dictionary with the response status and data
        """
        context = context or {}
        
        # Get parameters from config
        url = self.config.get("url", "")
        method = self.config.get("method", "POST")
        headers = self.config.get("headers", {})
        payload = self.config.get("payload", {})
        timeout = self.config.get("timeout", 30)
        
        # Check if URL is provided
        if not url:
            raise ValueError("URL is required")
        
        # If payload is a string, try to parse it as JSON
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                # If it's not valid JSON, keep it as a string
                pass
        
        # Make the webhook request
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            json=payload if isinstance(payload, dict) else None,
            data=payload if not isinstance(payload, dict) else None,
            timeout=timeout
        )
        
        # Check if the request was successful
        response.raise_for_status()
        
        # Return the response status and data
        try:
            response_data = response.json()
        except json.JSONDecodeError:
            response_data = response.text
        
        return {
            "status_code": response.status_code,
            "data": response_data
        }
