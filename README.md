# miniflow

A small, extendable workflow engine inspired by Metaflow.

## Quickstart

1. Create Virtual env:
   ```Python
   python -m venv .venv
   source .venv/bin/activate
   ```
2. Install:
   ```
   pip install -r requirements.txt
   ```

4. Run example workflow:
   ```
   python -m miniflow.cli run workflows.example_workflow:ExampleWorkflow
   ```

5. Run tests:
   ```
   pytest -q
   ```

## How to write workflows

Define a class inheriting `Workflow` and decorate methods with `@step(order=...)`.
Use `ctx.state` to pass values between steps.

## Extending storage

Implement `BaseStorage` in `miniflow.storage` and pass it to your workflow class:
`wf = MyWorkflow(storage=MyStorage())`
