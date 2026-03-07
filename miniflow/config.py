import os

class Config:
    STORAGE_BACKEND: str
    DATA_DIR: str

    def __init__(self) -> None:
        self.STORAGE_BACKEND = os.getenv("MINIFLOW_STORAGE", "file") 
        self.DATA_DIR = os.getenv("MINIFLOW_DATA_DIR", "./data")
        os.makedirs(self.DATA_DIR, exist_ok=True)

config = Config()