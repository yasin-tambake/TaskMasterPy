"""
Webhook-based triggers for TaskMasterPy.

This module defines triggers that fire based on incoming webhook requests.
"""
import threading
import time
import json
from typing import Dict, Any, Optional, List, Callable, Tuple
import uuid

from taskmaster.triggers.base import BaseTrigger

try:
    from flask import Flask, request, jsonify
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False


class WebhookTrigger(BaseTrigger):
    """A trigger that fires when a webhook endpoint is called.
    
    This trigger creates a Flask web server to listen for incoming
    webhook requests.
    """
    
    # Class-level Flask app and server thread
    _app: Optional[Flask] = None
    _server_thread: Optional[threading.Thread] = None
    _is_server_running = False
    _registered_endpoints: Dict[str, 'WebhookTrigger'] = {}
    _port = 5000
    _host = "0.0.0.0"
    
    @classmethod
    def _initialize_server(cls) -> None:
        """Initialize the Flask server if it's not already running."""
        if not FLASK_AVAILABLE:
            raise ImportError(
                "Flask is required for WebhookTrigger. "
                "Install it with 'pip install flask'."
            )
        
        if cls._app is None:
            cls._app = Flask("TaskMasterWebhookServer")
            
            @cls._app.route("/<path:endpoint_id>", methods=["GET", "POST", "PUT", "DELETE"])
            def webhook_endpoint(endpoint_id: str) -> Tuple[Dict[str, Any], int]:
                """Handle incoming webhook requests."""
                if endpoint_id not in cls._registered_endpoints:
                    return jsonify({"error": "Endpoint not found"}), 404
                
                trigger = cls._registered_endpoints[endpoint_id]
                
                # Extract data from the request
                if request.method in ["POST", "PUT"]:
                    if request.is_json:
                        data = request.get_json()
                    else:
                        data = request.form.to_dict()
                else:
                    data = request.args.to_dict()
                
                # Fire the trigger
                trigger.fire({
                    "method": request.method,
                    "endpoint_id": endpoint_id,
                    "data": data,
                    "headers": dict(request.headers),
                    "time": time.time()
                })
                
                # Return a success response
                return jsonify({
                    "status": "success",
                    "message": "Webhook received",
                    "endpoint_id": endpoint_id
                }), 200
    
    @classmethod
    def _start_server(cls) -> None:
        """Start the Flask server in a separate thread."""
        if cls._is_server_running:
            return
        
        def run_server() -> None:
            """Run the Flask server."""
            cls._app.run(host=cls._host, port=cls._port, debug=False, use_reloader=False)
        
        cls._server_thread = threading.Thread(target=run_server, daemon=True)
        cls._server_thread.start()
        cls._is_server_running = True
        
        # Give the server a moment to start
        time.sleep(1)
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new webhook trigger.
        
        Args:
            name: A unique name for this trigger
            config: Configuration parameters for the trigger
                - endpoint_id: A unique ID for the webhook endpoint
                - port: The port to listen on (default: 5000)
                - host: The host to bind to (default: 0.0.0.0)
                - require_auth: Whether to require authentication
                - auth_token: The token to use for authentication
        """
        super().__init__(name, config)
        self.endpoint_id = self.config.get("endpoint_id", str(uuid.uuid4()))
        self.require_auth = self.config.get("require_auth", False)
        self.auth_token = self.config.get("auth_token", "")
        
        # Update class-level port and host if specified
        if "port" in self.config:
            self.__class__._port = self.config["port"]
        if "host" in self.config:
            self.__class__._host = self.config["host"]
    
    def activate(self) -> None:
        """Activate the trigger to start listening for webhook requests."""
        super().activate()
        
        # Initialize and start the server if needed
        self.__class__._initialize_server()
        self.__class__._registered_endpoints[self.endpoint_id] = self
        
        if not self.__class__._is_server_running:
            self.__class__._start_server()
    
    def deactivate(self) -> None:
        """Deactivate the trigger to stop listening for webhook requests."""
        if self.endpoint_id in self.__class__._registered_endpoints:
            del self.__class__._registered_endpoints[self.endpoint_id]
        
        self.is_active = False
    
    def get_webhook_url(self) -> str:
        """Get the URL for this webhook endpoint.
        
        Returns:
            The URL for this webhook endpoint
        """
        return f"http://{self.__class__._host}:{self.__class__._port}/{self.endpoint_id}"
