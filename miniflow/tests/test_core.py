import os
from miniflow.storage import FileStorage
from workflows.example_workflow import ExampleWorkflow

def test_workflow_run(tmp_path, monkeypatch):
    storage = FileStorage(data_dir=str(tmp_path))
    wf = ExampleWorkflow(storage=storage)
    ctx = wf.run()
    assert ctx.status == "success"
    assert "fetch_data" in ctx.state
    assert "process" in ctx.state
    assert "save" in ctx.state
    # persisted file exists
    assert ctx.run_id in [p[:-5] for p in os.listdir(str(tmp_path))]