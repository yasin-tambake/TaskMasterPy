
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
from taskmaster.storage.db_storage import WorkflowStorage

app = typer.Typer(
    name="taskmaster",
    help="TaskMasterPy: A Python-based automation framework for data operations",
    add_completion=False
)

# Create subcommands
db_app = typer.Typer(
    name="db",
    help="Database operations for workflows",
    add_completion=False
)

app.add_typer(db_app, name="db")

console = Console()

# Initialize workflow storage
workflow_storage = WorkflowStorage()


@app.command("run")
def run_workflow(
    config_path: str = typer.Argument(..., help="Path to the workflow configuration file or workflow ID"),
    validate_only: bool = typer.Option(False, "--validate-only", "-v", help="Validate the configuration without running the workflow"),
    wait: bool = typer.Option(True, "--wait/--no-wait", help="Wait for the workflow to complete"),
    timeout: int = typer.Option(0, "--timeout", "-t", help="Timeout in seconds (0 for no timeout)"),
    use_db: bool = typer.Option(False, "--db", help="Treat config_path as a workflow ID in the database")
):
    """Run a workflow from a configuration file or from the database."""
    try:
        workflow = None

        if use_db or (not os.path.exists(config_path) and len(config_path) >= 8):
            # Try to load from database
            with console.status(f"Loading workflow {config_path} from database..."):
                workflow = workflow_storage.get_workflow_instance(config_path)

            if not workflow:
                console.print(f"[bold red]Error:[/bold red] Workflow with ID '{config_path}' not found in database")
                sys.exit(1)

            console.print(f"[bold green]Loaded workflow from database: {workflow.name}[/bold green]")
        else:
            # Load from file
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
                result_context = runner.run_workflow_now(workflow.id)

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
    config_dir: str = typer.Argument(".", help="Directory containing workflow configuration files"),
    include_db: bool = typer.Option(True, "--include-db/--no-db", help="Include workflows from the database")
):
    """List available workflows from files and/or database."""
    try:
        file_workflows = []
        db_workflows = []

        # Get workflows from files
        if os.path.exists(config_dir):
            # Find all YAML and JSON files in the directory
            workflow_files = []
            for root, _, files in os.walk(config_dir):
                for file in files:
                    if file.endswith((".yaml", ".yml", ".json")):
                        workflow_files.append(os.path.join(root, file))

            # Load and validate each workflow file
            for file_path in workflow_files:
                try:
                    config = load_workflow_config(file_path)
                    is_valid, _ = validate_workflow_config(config)

                    workflow_id = config.get("id", "")
                    name = config.get("name", "")
                    description = config.get("description", "")

                    file_workflows.append({
                        "id": workflow_id or "N/A",
                        "name": name or os.path.basename(file_path),
                        "description": description or "No description",
                        "path": file_path,
                        "valid": is_valid
                    })

                except Exception as e:
                    file_workflows.append({
                        "id": "N/A",
                        "name": os.path.basename(file_path),
                        "description": f"Error: {str(e)}",
                        "path": file_path,
                        "valid": False
                    })

        # Get workflows from database
        if include_db:
            db_workflows = workflow_storage.list_workflows()

        # Display file workflows
        if file_workflows:
            table = Table(title="File-Based Workflows")
            table.add_column("ID", style="cyan")
            table.add_column("Name", style="green")
            table.add_column("Description", style="blue")
            table.add_column("Path", style="yellow")
            table.add_column("Valid", style="magenta")

            for workflow in file_workflows:
                table.add_row(
                    workflow["id"],
                    workflow["name"],
                    workflow["description"],
                    workflow["path"],
                    "[green]Yes[/green]" if workflow["valid"] else "[red]No[/red]"
                )

            console.print(table)
        elif os.path.exists(config_dir):
            console.print(f"[bold yellow]No workflow configuration files found in {config_dir}[/bold yellow]")
        else:
            console.print(f"[bold red]Directory not found: {config_dir}[/bold red]")

        # Display database workflows
        if include_db:
            if db_workflows:
                table = Table(title="Database Workflows")
                table.add_column("ID", style="cyan")
                table.add_column("Name", style="green")
                table.add_column("Description", style="blue")
                table.add_column("Created", style="yellow")
                table.add_column("Updated", style="magenta")

                for workflow in db_workflows:
                    table.add_row(
                        workflow["id"],
                        workflow["name"],
                        workflow["description"] or "No description",
                        workflow["created_at"],
                        workflow["updated_at"]
                    )

                console.print(table)
            else:
                console.print("[bold yellow]No workflows found in the database.[/bold yellow]")

        # If no workflows found at all
        if not file_workflows and not db_workflows:
            console.print("[bold yellow]No workflows found.[/bold yellow]")

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
    wait: bool = typer.Option(True, "--wait/--no-wait", help="Wait for the workflow to complete"),
    use_db: bool = typer.Option(False, "--db", help="Look for the workflow in the database")
):
    """Manually trigger a workflow from a file or database."""
    try:
        workflow = None

        # Try to load from database first if specified
        if use_db:
            with console.status(f"Loading workflow {workflow_id} from database..."):
                workflow = workflow_storage.get_workflow_instance(workflow_id)

            if workflow:
                console.print(f"[bold green]Loaded workflow from database: {workflow.name}[/bold green]")

        # If not found in database or not using database, try files
        if not workflow:
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

            if not workflow_path and not use_db:
                # If not using DB and not found in files, try DB as a fallback
                with console.status(f"Looking for workflow {workflow_id} in database..."):
                    workflow = workflow_storage.get_workflow_instance(workflow_id)

                if not workflow:
                    console.print(f"[bold red]Error:[/bold red] Workflow with ID '{workflow_id}' not found")
                    sys.exit(1)

                console.print(f"[bold green]Loaded workflow from database: {workflow.name}[/bold green]")
            elif workflow_path:
                # Load the workflow configuration if needed
                if not workflow_config:
                    workflow_config = load_workflow_config(workflow_path)

                # Create the workflow
                with console.status("Creating workflow..."):
                    workflow = load_workflow_from_config(workflow_config)

                console.print(f"[bold green]Loaded workflow from file: {workflow.name}[/bold green]")
            else:
                console.print(f"[bold red]Error:[/bold red] Workflow with ID '{workflow_id}' not found")
                sys.exit(1)

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
                result_context = runner.run_workflow_now(workflow.id)

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


# Database commands

@db_app.command("save")
def save_workflow_to_db(
    workflow_id: str = typer.Argument(..., help="ID of the workflow to save"),
    config: Dict[str, Any] = typer.Option(None, "--config", "-c", help="Workflow configuration as JSON string")
):
    """Save a workflow to the database."""
    try:
        if config:
            # Save the provided configuration
            workflow_storage.save_workflow(workflow_id, config)
            console.print(f"[bold green]Workflow saved to database with ID: {workflow_id}[/bold green]")
        else:
            console.print("[bold red]Error:[/bold red] No configuration provided")
            sys.exit(1)

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        sys.exit(1)


@db_app.command("list")
def list_db_workflows():
    """List all workflows in the database."""
    try:
        workflows = workflow_storage.list_workflows()

        if not workflows:
            console.print("[bold yellow]No workflows found in the database.[/bold yellow]")
            return

        # Create a table to display the workflows
        table = Table(title="Workflows in Database")
        table.add_column("ID", style="cyan")
        table.add_column("Name", style="green")
        table.add_column("Description", style="blue")
        table.add_column("Created", style="yellow")
        table.add_column("Updated", style="magenta")

        for workflow in workflows:
            table.add_row(
                workflow["id"],
                workflow["name"],
                workflow["description"] or "No description",
                workflow["created_at"],
                workflow["updated_at"]
            )

        console.print(table)

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        sys.exit(1)


@db_app.command("import")
def import_workflow(
    file_path: str = typer.Argument(..., help="Path to the workflow configuration file")
):
    """Import a workflow from a file into the database."""
    try:
        # Check if the file exists
        if not os.path.exists(file_path):
            console.print(f"[bold red]Error:[/bold red] File not found: {file_path}")
            sys.exit(1)

        # Import the workflow
        with console.status(f"Importing workflow from {file_path}..."):
            workflow_id = workflow_storage.import_from_file(file_path)

        console.print(f"[bold green]Workflow imported successfully with ID: {workflow_id}[/bold green]")

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        sys.exit(1)


@db_app.command("export")
def export_workflow(
    workflow_id: str = typer.Argument(..., help="ID of the workflow to export"),
    file_path: str = typer.Argument(..., help="Path to save the workflow configuration")
):
    """Export a workflow from the database to a file."""
    try:
        # Export the workflow
        with console.status(f"Exporting workflow {workflow_id} to {file_path}..."):
            success = workflow_storage.export_to_file(workflow_id, file_path)

        if success:
            console.print(f"[bold green]Workflow exported successfully to: {file_path}[/bold green]")
        else:
            console.print(f"[bold red]Error:[/bold red] Workflow with ID '{workflow_id}' not found")
            sys.exit(1)

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        sys.exit(1)


@db_app.command("delete")
def delete_workflow(
    workflow_id: str = typer.Argument(..., help="ID of the workflow to delete"),
    force: bool = typer.Option(False, "--force", "-f", help="Force deletion without confirmation")
):
    """Delete a workflow from the database."""
    try:
        if not force:
            # Confirm deletion
            confirm = typer.confirm(f"Are you sure you want to delete workflow '{workflow_id}'?")
            if not confirm:
                console.print("[bold yellow]Deletion cancelled.[/bold yellow]")
                return

        # Delete the workflow
        with console.status(f"Deleting workflow {workflow_id}..."):
            success = workflow_storage.delete_workflow(workflow_id)

        if success:
            console.print(f"[bold green]Workflow deleted successfully.[/bold green]")
        else:
            console.print(f"[bold red]Error:[/bold red] Workflow with ID '{workflow_id}' not found")
            sys.exit(1)

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        sys.exit(1)


@db_app.command("run")
def run_db_workflow(
    workflow_id: str = typer.Argument(..., help="ID of the workflow to run"),
    wait: bool = typer.Option(True, "--wait/--no-wait", help="Wait for the workflow to complete")
):
    """Run a workflow from the database."""
    try:
        # Load the workflow
        with console.status(f"Loading workflow {workflow_id}..."):
            workflow = workflow_storage.get_workflow_instance(workflow_id)

        if not workflow:
            console.print(f"[bold red]Error:[/bold red] Workflow with ID '{workflow_id}' not found")
            sys.exit(1)

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
                result_context = runner.run_workflow_now(workflow.id)

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


if __name__ == "__main__":
    app()

