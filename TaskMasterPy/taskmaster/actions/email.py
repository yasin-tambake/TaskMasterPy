"""
Email actions for TaskMasterPy.

This module defines actions for sending email notifications.
"""
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from typing import Dict, Any, Optional, List, Union

from taskmaster.actions.base import BaseAction


class SendEmailAction(BaseAction):
    """Action to send an email notification."""
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new send email action.
        
        Args:
            name: A unique name for this action
            config: Configuration parameters for the action
                - smtp_server: SMTP server address
                - smtp_port: SMTP server port (default: 587)
                - use_tls: Whether to use TLS (default: True)
                - username: SMTP username
                - password: SMTP password
                - from_email: Sender email address
                - to_email: Recipient email address(es)
                - cc_email: CC email address(es)
                - bcc_email: BCC email address(es)
                - subject: Email subject
                - body: Email body
                - body_type: Body type (default: 'plain')
                  Options: 'plain', 'html'
                - attachments: List of file paths to attach
        """
        super().__init__(name, config)
    
    def execute(self, context: Dict[str, Any] = None) -> bool:
        """Execute the action to send an email.
        
        Args:
            context: Execution context
            
        Returns:
            True if the email was sent successfully, False otherwise
        """
        context = context or {}
        
        # Get parameters from config
        smtp_server = self.config.get("smtp_server", "")
        smtp_port = self.config.get("smtp_port", 587)
        use_tls = self.config.get("use_tls", True)
        username = self.config.get("username", "")
        password = self.config.get("password", "")
        
        from_email = self.config.get("from_email", "")
        to_email = self.config.get("to_email", [])
        cc_email = self.config.get("cc_email", [])
        bcc_email = self.config.get("bcc_email", [])
        
        subject = self.config.get("subject", "")
        body = self.config.get("body", "")
        body_type = self.config.get("body_type", "plain")
        
        attachments = self.config.get("attachments", [])
        
        # Convert single email addresses to lists
        if isinstance(to_email, str):
            to_email = [to_email]
        if isinstance(cc_email, str):
            cc_email = [cc_email]
        if isinstance(bcc_email, str):
            bcc_email = [bcc_email]
        
        # Check required parameters
        if not smtp_server:
            raise ValueError("SMTP server is required")
        if not from_email:
            raise ValueError("Sender email is required")
        if not to_email:
            raise ValueError("Recipient email is required")
        
        try:
            # Create the email message
            msg = MIMEMultipart()
            msg["From"] = from_email
            msg["To"] = ", ".join(to_email)
            if cc_email:
                msg["Cc"] = ", ".join(cc_email)
            msg["Subject"] = subject
            
            # Add the body
            msg.attach(MIMEText(body, body_type))
            
            # Add attachments
            for attachment in attachments:
                if os.path.exists(attachment):
                    with open(attachment, "rb") as f:
                        part = MIMEApplication(f.read(), Name=os.path.basename(attachment))
                    
                    part["Content-Disposition"] = f'attachment; filename="{os.path.basename(attachment)}"'
                    msg.attach(part)
            
            # Connect to the SMTP server
            server = smtplib.SMTP(smtp_server, smtp_port)
            if use_tls:
                server.starttls()
            
            # Login if credentials are provided
            if username and password:
                server.login(username, password)
            
            # Send the email
            all_recipients = to_email + cc_email + bcc_email
            server.sendmail(from_email, all_recipients, msg.as_string())
            
            # Close the connection
            server.quit()
            
            return True
        
        except Exception as e:
            print(f"Error sending email: {str(e)}")
            self.error = e
            return False
