from miniflow.storage import FileStorage

def test_file_storage(tmp_path):
    s = FileStorage(data_dir=str(tmp_path))
    s.save_run("r1", {"a": 1})
    data = s.load_run("r1")
    assert data["a"] == 1
    assert "r1" in s.list_runs()