import json
import os
from datetime import datetime
from typing import Any, Dict


class BaseStorage:
    def save_state(self, state: Dict[str, Any]) -> None:
        pass

    def retrieve_state(self) -> Dict[str, Any]:
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: Dict[str, Any]) -> None:
        with open(self.file_path, 'w') as file:
            json.dump(state, file)

    def retrieve_state(self) -> Dict[str, Any]:
        if os.path.exists(self.file_path):
            with open(self.file_path, 'r') as file:
                return json.load(file)
        else:
            return {}


class State:
    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        if isinstance(value, datetime):
            value = value.isoformat()
        state = self.storage.retrieve_state()
        state[key] = value
        self.storage.save_state(state)

    def get_state(self, key: str) -> Any:
        state = self.storage.retrieve_state()
        value = state.get(key, None)
        return value
