import json 
import os 
import sqlite3
from abc import ABC, abstractmethod
from typing import Any , Dict , List , Optional
from .config import config

class BaseStorage(ABC):
    @abstractmethod
    def save_run(self , run_id: str , payload: Dict[str , Any]) -> None:
        ...
    
    @abstractmethod
    def load_run(self , run_id: str) -> Optional[Dict[str , Any]]:
        ...
    
    @abstractmethod
    def list_runs(self) -> List[str]:
        ...
    

class FileStorage(BaseStorage):
    def __init__(self , data_dir: str = config.DATA_DIR):
        self.data_dir = data_dir
        os.makedirs(self.data_dir , exist_ok=True)
    
    def _path(self , run_id: str) -> str:
        return os.path.join(self.data_dir , f"{run_id}.json")
    
    def save_run(self , run_id:str , payload : Dict[str , Any]) -> None:
        with open(self._path(run_id) , "w" , encoding="utf-8") as f:
            json.dump(payload , f , default=str , indent = 2)
    
    def load_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        path = self._path(run_id)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def list_runs(self) -> List[str]:
        files = []

        for f in os.listdir(self.data_dir):
            if f.endswith(".json"):
                files.append(f[:-5])
        
        return files


def get_storage(backend: str = config.STORAGE_BACKEND) -> BaseStorage:
    if backend == "sqlite":
        return SQLiteStorage()
    return FileStorage()