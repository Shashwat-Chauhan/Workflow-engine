import os
from typing import Optional

class Config:
    STORAGE_BACKEND: str
    DATA_DIR: str

    def __init__(self) -> None:
            