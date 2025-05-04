"""
Database storage for TaskMasterPy workflows.

This module provides a database-backed storage implementation for workflows.
"""
import os
import json
import sqlite3
import yaml
from typing import Dict, Any, List, Optional, Tuple, Union
import logging

from taskmaster.core.workflow import Workflow
from taskmaster.utils.config import load_workflow_from_config


class WorkflowStorage:
    """Database storage for workflows.
    
    This class provides methods to save, load, list, and delete workflows
    using a SQLite database as the backend.
    """
    
    def __init__(self, db_path: str = None):
        """Initialize a new workflow storage.
        
        Args:
            db_path: Path to the SQLite database file. If None, uses the default
                    location in the user's home directory.
        """
        if db_path is None:
            # Use default location in user's home directory
            home_dir = os.path.expanduser("~")
            taskmaster_dir = os.path.join(home_dir, ".taskmaster")
            os.makedirs(taskmaster_dir, exist_ok=True)
            db_path = os.path.join(taskmaster_dir, "workflows.db")
        
        self.db_path = db_path
        self.logger = logging.getLogger("taskmaster.storage")
        
        # Initialize the database
        self._init_db()
    
    def _init_db(self) -> None:
        """Initialize the database schema."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create the workflows table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS workflows (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            config TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        conn.commit()
        conn.close()
        
        self.logger.info(f"Initialized workflow database at {self.db_path}")
    
    def save_workflow(self, workflow_id: str, config: Dict[str, Any]) -> None:
        """Save a workflow configuration to the database.
        
        Args:
            workflow_id: The ID of the workflow
            config: The workflow configuration as a dictionary
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check if the workflow already exists
        cursor.execute("SELECT id FROM workflows WHERE id = ?", (workflow_id,))
        exists = cursor.fetchone() is not None
        
        # Convert the config to JSON
        config_json = json.dumps(config)
        
        if exists:
            # Update the existing workflow
            cursor.execute("""
            UPDATE workflows
            SET name = ?, description = ?, config = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """, (
                config.get("name", ""),
                config.get("description", ""),
                config_json,
                workflow_id
            ))
            self.logger.info(f"Updated workflow {workflow_id} in database")
        else:
            # Insert a new workflow
            cursor.execute("""
            INSERT INTO workflows (id, name, description, config)
            VALUES (?, ?, ?, ?)
            """, (
                workflow_id,
                config.get("name", ""),
                config.get("description", ""),
                config_json
            ))
            self.logger.info(f"Saved workflow {workflow_id} to database")
        
        conn.commit()
        conn.close()
    
    def load_workflow(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Load a workflow configuration from the database.
        
        Args:
            workflow_id: The ID of the workflow
            
        Returns:
            The workflow configuration as a dictionary, or None if not found
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT config FROM workflows WHERE id = ?", (workflow_id,))
        result = cursor.fetchone()
        
        conn.close()
        
        if result:
            config = json.loads(result[0])
            self.logger.info(f"Loaded workflow {workflow_id} from database")
            return config
        
        self.logger.warning(f"Workflow {workflow_id} not found in database")
        return None
    
    def delete_workflow(self, workflow_id: str) -> bool:
        """Delete a workflow from the database.
        
        Args:
            workflow_id: The ID of the workflow
            
        Returns:
            True if the workflow was deleted, False if it wasn't found
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM workflows WHERE id = ?", (workflow_id,))
        deleted = cursor.rowcount > 0
        
        conn.commit()
        conn.close()
        
        if deleted:
            self.logger.info(f"Deleted workflow {workflow_id} from database")
        else:
            self.logger.warning(f"Workflow {workflow_id} not found for deletion")
        
        return deleted
    
    def list_workflows(self) -> List[Dict[str, Any]]:
        """List all workflows in the database.
        
        Returns:
            A list of workflow metadata dictionaries
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
        SELECT id, name, description, created_at, updated_at
        FROM workflows
        ORDER BY name
        """)
        
        workflows = []
        for row in cursor.fetchall():
            workflows.append({
                "id": row[0],
                "name": row[1],
                "description": row[2],
                "created_at": row[3],
                "updated_at": row[4]
            })
        
        conn.close()
        
        self.logger.info(f"Listed {len(workflows)} workflows from database")
        return workflows
    
    def import_from_file(self, file_path: str) -> str:
        """Import a workflow from a file into the database.
        
        Args:
            file_path: Path to the workflow configuration file
            
        Returns:
            The ID of the imported workflow
        """
        # Load the workflow configuration
        with open(file_path, "r") as f:
            if file_path.endswith((".yaml", ".yml")):
                config = yaml.safe_load(f)
            elif file_path.endswith(".json"):
                config = json.load(f)
            else:
                raise ValueError(f"Unsupported file format: {file_path}")
        
        # Ensure the workflow has an ID
        if "id" not in config:
            # Create a workflow object to get an ID
            workflow = load_workflow_from_config(config)
            config["id"] = workflow.id
        
        # Save the workflow to the database
        self.save_workflow(config["id"], config)
        
        self.logger.info(f"Imported workflow from {file_path} to database")
        return config["id"]
    
    def export_to_file(self, workflow_id: str, file_path: str) -> bool:
        """Export a workflow from the database to a file.
        
        Args:
            workflow_id: The ID of the workflow
            file_path: Path to save the workflow configuration
            
        Returns:
            True if the workflow was exported, False if it wasn't found
        """
        # Load the workflow configuration
        config = self.load_workflow(workflow_id)
        if not config:
            return False
        
        # Save the configuration to a file
        with open(file_path, "w") as f:
            if file_path.endswith((".yaml", ".yml")):
                yaml.dump(config, f, default_flow_style=False)
            elif file_path.endswith(".json"):
                json.dump(config, f, indent=2)
            else:
                raise ValueError(f"Unsupported file format: {file_path}")
        
        self.logger.info(f"Exported workflow {workflow_id} to {file_path}")
        return True
    
    def get_workflow_instance(self, workflow_id: str) -> Optional[Workflow]:
        """Get a workflow instance from the database.
        
        Args:
            workflow_id: The ID of the workflow
            
        Returns:
            A workflow instance, or None if not found
        """
        config = self.load_workflow(workflow_id)
        if not config:
            return None
        
        workflow = load_workflow_from_config(config)
        return workflow
