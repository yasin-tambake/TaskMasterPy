"""
Script execution actions for TaskMasterPy.

This module defines actions for running custom scripts, such as
Python scripts or shell scripts.
"""
import os
import sys
import subprocess
import importlib.util
from typing import Dict, Any, Optional, List, Union
import pandas as pd

from taskmaster.actions.base import BaseAction


class RunScriptAction(BaseAction):
    """Base class for actions that run scripts."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new run script action.
        
        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
        """
        super().__init__(name, config)
    
    def execute(self, context: Dict[str, Any] = None) -> Any:
        """Execute the action to run a script.
        
        Args:
            context: Execution context
            
        Returns:
            The result of the script execution
        """
        raise NotImplementedError("Subclasses must implement execute()")


class RunPythonScriptAction(RunScriptAction):
    """Action to run a Python script."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new run Python script action.
        
        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
                - script_path: Path to the Python script file
                - function_name: Name of the function to call (default: 'main')
                - args: Positional arguments to pass to the function
                - kwargs: Keyword arguments to pass to the function
                - pass_context: Whether to pass the context to the function (default: False)
                - return_dataframe: Whether the function returns a DataFrame (default: False)
        """
        super().__init__(name, config)
    
    def execute(self, context: Dict[str, Any] = None) -> Any:
        """Execute the action to run a Python script.
        
        Args:
            context: Execution context
            
        Returns:
            The result of the script execution
        """
        context = context or {}
        
        # Get parameters from config
        script_path = self.config.get("script_path", "")
        function_name = self.config.get("function_name", "main")
        args = self.config.get("args", [])
        kwargs = self.config.get("kwargs", {})
        pass_context = self.config.get("pass_context", False)
        return_dataframe = self.config.get("return_dataframe", False)
        
        # Check if script path is provided
        if not script_path:
            raise ValueError("script_path is required")
        
        # Check if the script file exists
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"Script file not found: {script_path}")
        
        # Load the script module
        spec = importlib.util.spec_from_file_location("script_module", script_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Could not load script: {script_path}")
        
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # Check if the function exists in the module
        if not hasattr(module, function_name):
            raise AttributeError(f"Function {function_name} not found in script {script_path}")
        
        # Get the function
        func = getattr(module, function_name)
        
        # Prepare arguments
        call_args = list(args)
        call_kwargs = dict(kwargs)
        
        # Add context if requested
        if pass_context:
            call_kwargs["context"] = context
        
        # Call the function
        result = func(*call_args, **call_kwargs)
        
        # Convert to DataFrame if requested
        if return_dataframe and not isinstance(result, pd.DataFrame):
            if isinstance(result, dict):
                result = pd.DataFrame([result])
            elif isinstance(result, list):
                result = pd.DataFrame(result)
            else:
                raise TypeError(f"Cannot convert result to DataFrame: {type(result)}")
        
        return result


class RunShellScriptAction(RunScriptAction):
    """Action to run a shell script."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new run shell script action.
        
        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
                - script_path: Path to the shell script file
                - args: Arguments to pass to the script
                - shell: Whether to run the command through the shell (default: True)
                - cwd: Working directory for the command
                - env: Environment variables for the command
                - timeout: Timeout in seconds (default: 60)
                - capture_output: Whether to capture stdout/stderr (default: True)
                - check: Whether to check the return code (default: True)
                - return_stdout: Whether to return stdout as a string (default: True)
                - return_dataframe: Whether to parse stdout as CSV and return a DataFrame (default: False)
        """
        super().__init__(name, config)
    
    def execute(self, context: Dict[str, Any] = None) -> Any:
        """Execute the action to run a shell script.
        
        Args:
            context: Execution context
            
        Returns:
            The result of the script execution
        """
        context = context or {}
        
        # Get parameters from config
        script_path = self.config.get("script_path", "")
        args = self.config.get("args", [])
        shell = self.config.get("shell", True)
        cwd = self.config.get("cwd")
        env = self.config.get("env")
        timeout = self.config.get("timeout", 60)
        capture_output = self.config.get("capture_output", True)
        check = self.config.get("check", True)
        return_stdout = self.config.get("return_stdout", True)
        return_dataframe = self.config.get("return_dataframe", False)
        
        # Check if script path is provided
        if not script_path:
            raise ValueError("script_path is required")
        
        # Check if the script file exists
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"Script file not found: {script_path}")
        
        # Prepare the command
        command = [script_path] + args
        
        # Run the command
        result = subprocess.run(
            command,
            shell=shell,
            cwd=cwd,
            env=env,
            timeout=timeout,
            capture_output=capture_output,
            check=check,
            text=True
        )
        
        # Process the result
        if return_dataframe:
            # Parse stdout as CSV and return a DataFrame
            return pd.read_csv(pd.StringIO(result.stdout))
        elif return_stdout:
            # Return stdout as a string
            return result.stdout
        else:
            # Return the CompletedProcess object
            return {
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr
            }
