
"""
CLI commands for TaskMasterPy.

This module defines the command-line interface for TaskMasterPy.
"""
import os
import sys
import time
import yaml
import json
from typing import Dict, Any, Optional, List, Union
import typer
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

from taskmaster.core.workflow import Workflow
from taskmaster.core.runner import WorkflowRunner
from taskmaster.utils.config import load_workflow_from_config
from taskmaster.utils.validators import validate_workflow_config

app = typer.Typer(
    name="taskmaster",
    help="TaskMasterPy: A Python-based automation framework for data operations",
    add_completion=False
)

console = Console()


@app.command("run")
def run_workflow(
    config_path: str = typer.Argument(..., help="Path to the workflow configuration file"),
    validate_only: bool = typer.Option(False, "--validate-only", "-v", help="Validate the configuration without running the workflow"),
    wait: bool = typer.Option(True, "--wait/--no-wait", help="Wait for the workflow to complete"),
    timeout: int = typer.Option(0, "--timeout", "-t", help="Timeout in seconds (0 for no timeout)")
):
    """Run a workflow from a configuration file."""
    try:
        # Check if the config file exists
        if not os.path.exists(config_path):
            console.print(f"[bold red]Error:[/bold red] Configuration file not found: {config_path}")
            sys.exit(1)
        
        # Load the configuration
        with console.status(f"Loading configuration from {config_path}..."):
            config = load_workflow_config(config_path)
        
        # Validate the configuration
        with console.status("Validating configuration..."):
            is_valid, errors = validate_workflow_config(config)
        
        if not is_valid:
            console.print("[bold red]Configuration validation failed:[/bold red]")
            for error in errors:
                console.print(f"  - {error}")
            sys.exit(1)
        
        console.print("[bold green]Configuration is valid.[/bold green]")
        
        if validate_only:
            return
        
        # Create the workflow
        with console.status("Creating workflow..."):
            workflow = load_workflow_from_config(config)
        
        # Create a runner
        runner = WorkflowRunner()
        runner.register_workflow(workflow)
        
        # Run the workflow
        console.print(f"[bold blue]Running workflow: {workflow.name}[/bold blue]")
        
        start_time = time.time()
        
        if wait:
            with Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]Running workflow...[/bold blue]"),
                console=console
            ) as progress:
                task = progress.add_task("Running", total=None)
                
                # Run the workflow
                context = runner.run_workflow_now(workflow.id)
                
                progress.update(task, completed=True)
            
            # Print the results
            elapsed_time = time.time() - start_time
            console.print(f"[bold green]Workflow completed in {elapsed_time:.2f} seconds.[/bold green]")
            
            # Check if any actions failed
            failed_actions = [
                action for action in workflow.actions.values()
                if action.status == "failed"
            ]
            
            if failed_actions:
                console.print(f"[bold red]{len(failed_actions)} action(s) failed:[/bold red]")
                for action in failed_actions:
                    console.print(f"  - {action.name}: {action.error}")
                sys.exit(1)
            
        else:
            # Start the workflow and return immediately
            runner.start_workflow(workflow.id)
            console.print("[bold yellow]Workflow started in the background.[/bold yellow]")
    
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        sys.exit(1)


@app.command("list-workflows")
def list_workflows(
    config_dir: str = typer.Argument(".", help="Directory containing workflow configuration files")
):
    """List available workflows."""
    try:
        # Check if the directory exists
        if not os.path.exists(config_dir):
            console.print(f"[bold red]Error:[/bold red] Directory not found: {config_dir}")
            sys.exit(1)
        
        # Find all YAML and JSON files in the directory
        workflow_files = []
        for root, _, files in os.walk(config_dir):
            for file in files:
                if file.endswith((".yaml", ".yml", ".json")):
                    workflow_files.append(os.path.join(root, file))
        
        if not workflow_files:
            console.print(f"[bold yellow]No workflow configuration files found in {config_dir}[/bold yellow]")
            return
        
        # Create a table to display the workflows
        table = Table(title="Available Workflows")
        table.add_column("ID", style="cyan")
        table.add_column("Name", style="green")
        table.add_column("Description", style="blue")
        table.add_column("Path", style="yellow")
        table.add_column("Valid", style="magenta")
        
        # Load and validate each workflow
        for file_path in workflow_files:
            try:
                config = load_workflow_config(file_path)
                is_valid, _ = validate_workflow_config(config)
                
                workflow_id = config.get("id", "")
                name = config.get("name", "")
                description = config.get("description", "")
                
                table.add_row(
                    workflow_id or "N/A",
                    name or os.path.basename(file_path),
                    description or "No description",
                    file_path,
                    "[green]Yes[/green]" if is_valid else "[red]No[/red]"
                )
            
            except Exception as e:
                table.add_row(
                    "N/A",
                    os.path.basename(file_path),
                    f"Error: {str(e)}",
                    file_path,
                    "[red]No[/red]"
                )
        
        console.print(table)
    
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        sys.exit(1)


@app.command("validate")
def validate_workflow(
    config_path: str = typer.Argument(..., help="Path to the workflow configuration file")
):
    """Validate a workflow configuration."""
    try:
        # Check if the config file exists
        if not os.path.exists(config_path):
            console.print(f"[bold red]Error:[/bold red] Configuration file not found: {config_path}")
            sys.exit(1)
        
        # Load the configuration
        with console.status(f"Loading configuration from {config_path}..."):
            config = load_workflow_config(config_path)
        
        # Validate the configuration
        with console.status("Validating configuration..."):
            is_valid, errors = validate_workflow_config(config)
        
        if is_valid:
            console.print("[bold green]Configuration is valid.[/bold green]")
        else:
            console.print("[bold red]Configuration validation failed:[/bold red]")
            for error in errors:
                console.print(f"  - {error}")
            sys.exit(1)
    
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        sys.exit(1)


@app.command("trigger-now")
def trigger_workflow(
    workflow_id: str = typer.Argument(..., help="ID of the workflow to trigger"),
    config_dir: str = typer.Option(".", "--config-dir", "-c", help="Directory containing workflow configuration files"),
    wait: bool = typer.Option(True, "--wait/--no-wait", help="Wait for the workflow to complete")
):
    """Manually trigger a workflow."""
    try:
        # Find the workflow configuration
        workflow_config = None
        workflow_path = None
        
        # Check if the workflow_id is a file path
        if os.path.exists(workflow_id):
            workflow_path = workflow_id
        else:
            # Search for the workflow in the config directory
            for root, _, files in os.walk(config_dir):
                for file in files:
                    if file.endswith((".yaml", ".yml", ".json")):
                        file_path = os.path.join(root, file)
                        try:
                            config = load_workflow_config(file_path)
                            if config.get("id") == workflow_id:
                                workflow_config = config
                                workflow_path = file_path
                                break
                        except:
                            pass
                
                if workflow_config:
                    break
        
        if not workflow_path:
            console.print(f"[bold red]Error:[/bold red] Workflow with ID '{workflow_id}' not found")
            sys.exit(1)
        
        # Load the workflow configuration if needed
        if not workflow_config:
            workflow_config = load_workflow_config(workflow_path)
        
        # Create the workflow
        with console.status("Creating workflow..."):
            workflow = load_workflow_from_config(workflow_config)
        
        # Create a runner
        runner = WorkflowRunner()
        runner.register_workflow(workflow)
        
        # Run the workflow
        console.print(f"[bold blue]Triggering workflow: {workflow.name}[/bold blue]")
        
        start_time = time.time()
        
        if wait:
            with Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]Running workflow...[/bold blue]"),
                console=console
            ) as progress:
                task = progress.add_task("Running", total=None)
                
                # Run the workflow
                context = runner.run_workflow_now(workflow.id)
                
                progress.update(task, completed=True)
            
            # Print the results
            elapsed_time = time.time() - start_time
            console.print(f"[bold green]Workflow completed in {elapsed_time:.2f} seconds.[/bold green]")
            
            # Check if any actions failed
            failed_actions = [
                action for action in workflow.actions.values()
                if action.status == "failed"
            ]
            
            if failed_actions:
                console.print(f"[bold red]{len(failed_actions)} action(s) failed:[/bold red]")
                for action in failed_actions:
                    console.print(f"  - {action.name}: {action.error}")
                sys.exit(1)
            
        else:
            # Start the workflow and return immediately
            runner.start_workflow(workflow.id)
            console.print("[bold yellow]Workflow started in the background.[/bold yellow]")
    
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        sys.exit(1)


def load_workflow_config(config_path: str) -> Dict[str, Any]:
    """Load a workflow configuration from a file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        The workflow configuration as a dictionary
    """
    with open(config_path, "r") as f:
        if config_path.endswith((".yaml", ".yml")):
            return yaml.safe_load(f)
        elif config_path.endswith(".json"):
            return json.load(f)
        else:
            raise ValueError(f"Unsupported file format: {config_path}")


if __name__ == "__main__":
    app()

