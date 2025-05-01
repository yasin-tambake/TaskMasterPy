"""
Notification actions for TaskMasterPy.

This module defines actions for sending notifications.
"""
import os
import sys
import subprocess
from typing import Dict, Any, Optional, List, Union

from taskmaster.actions.base import BaseAction


class NotifyAction(BaseAction):
    """Base class for actions that send notifications."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new notify action.
        
        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
        """
        super().__init__(name, config)
    
    def execute(self, context: Dict[str, Any] = None) -> bool:
        """Execute the action to send a notification.
        
        Args:
            context: Execution context
            
        Returns:
            True if the notification was sent successfully, False otherwise
        """
        raise NotImplementedError("Subclasses must implement execute()")


class ConsoleNotifyAction(NotifyAction):
    """Action to send a notification to the console."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new console notify action.
        
        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
                - message: The message to display
                - level: The message level (default: 'info')
                  Options: 'info', 'warning', 'error', 'success'
        """
        super().__init__(name, config)
    
    def execute(self, context: Dict[str, Any] = None) -> bool:
        """Execute the action to send a console notification.
        
        Args:
            context: Execution context
            
        Returns:
            True if the notification was sent successfully, False otherwise
        """
        context = context or {}
        
        # Get parameters from config
        message = self.config.get("message", "")
        level = self.config.get("level", "info")
        
        # Format the message based on the level
        if level == "info":
            prefix = "[INFO] "
        elif level == "warning":
            prefix = "[WARNING] "
        elif level == "error":
            prefix = "[ERROR] "
        elif level == "success":
            prefix = "[SUCCESS] "
        else:
            prefix = ""
        
        # Print the message
        print(f"{prefix}{message}")
        
        return True


class SystemNotifyAction(NotifyAction):
    """Action to send a system notification."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new system notify action.
        
        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
                - title: The notification title
                - message: The notification message
                - icon: Path to an icon file
        """
        super().__init__(name, config)
    
    def execute(self, context: Dict[str, Any] = None) -> bool:
        """Execute the action to send a system notification.
        
        Args:
            context: Execution context
            
        Returns:
            True if the notification was sent successfully, False otherwise
        """
        context = context or {}
        
        # Get parameters from config
        title = self.config.get("title", "TaskMasterPy Notification")
        message = self.config.get("message", "")
        icon = self.config.get("icon", "")
        
        # Send the notification based on the platform
        try:
            if sys.platform == "win32":
                # Windows notification
                self._notify_windows(title, message)
            elif sys.platform == "darwin":
                # macOS notification
                self._notify_macos(title, message)
            else:
                # Linux notification
                self._notify_linux(title, message, icon)
            
            return True
        
        except Exception as e:
            print(f"Error sending system notification: {str(e)}")
            self.error = e
            return False
    
    def _notify_windows(self, title: str, message: str) -> None:
        """Send a notification on Windows.
        
        Args:
            title: The notification title
            message: The notification message
        """
        try:
            # Try to use the win10toast library if available
            from win10toast import ToastNotifier
            toaster = ToastNotifier()
            toaster.show_toast(title, message, duration=5)
        except ImportError:
            # Fall back to a PowerShell command
            powershell_cmd = f"""
            [Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] | Out-Null
            [Windows.Data.Xml.Dom.XmlDocument, Windows.Data.Xml.Dom.XmlDocument, ContentType = WindowsRuntime] | Out-Null
            
            $app_id = 'TaskMasterPy'
            $template = @"
            <toast>
                <visual>
                    <binding template="ToastText02">
                        <text id="1">{title}</text>
                        <text id="2">{message}</text>
                    </binding>
                </visual>
            </toast>
            "@
            
            $xml = New-Object Windows.Data.Xml.Dom.XmlDocument
            $xml.LoadXml($template)
            $toast = [Windows.UI.Notifications.ToastNotification]::new($xml)
            [Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier($app_id).Show($toast)
            """
            
            subprocess.run(["powershell", "-Command", powershell_cmd], capture_output=True)
    
    def _notify_macos(self, title: str, message: str) -> None:
        """Send a notification on macOS.
        
        Args:
            title: The notification title
            message: The notification message
        """
        # Use the built-in osascript command
        applescript = f'display notification "{message}" with title "{title}"'
        subprocess.run(["osascript", "-e", applescript], capture_output=True)
    
    def _notify_linux(self, title: str, message: str, icon: str = "") -> None:
        """Send a notification on Linux.
        
        Args:
            title: The notification title
            message: The notification message
            icon: Path to an icon file
        """
        # Use the notify-send command
        cmd = ["notify-send", title, message]
        if icon and os.path.exists(icon):
            cmd.extend(["--icon", icon])
        
        subprocess.run(cmd, capture_output=True)
