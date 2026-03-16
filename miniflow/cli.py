# miniflow/cli.py
import importlib
import click
from .logger import get_logger
from .storage import get_storage
from .config import config

logger = get_logger("miniflow.cli")

@click.group()
def cli():
    pass

@cli.command()
@click.argument("workflow_class")
@click.option("--run-id", default=None, help="Existing run id to resume")
def run(workflow_class: str, run_id: str | None):
    """
    Run a workflow.

    WORKFLOW_CLASS format: module.path:ClassName
    Example: workflows.example_workflow:ExampleWorkflow
    """
    if ":" not in workflow_class:
        raise click.BadParameter("workflow_class must be module:path, e.g. workflows.mod:MyWorkflow")
    module_name, class_name = workflow_class.split(":")
    module = importlib.import_module(module_name)
    cls = getattr(module, class_name)
    wf = cls(storage=get_storage())
    logger.info("Starting workflow %s", workflow_class)
    ctx = wf.run(run_id=run_id)
    logger.info("Finished run %s with status %s", ctx.run_id, ctx.status)

if __name__ == "__main__":
    cli()