# utils.py
import json
import os
import time
from typing import Any, Dict

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path: str, data: Any) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)

def now_ts() -> int:
    return int(time.time())

def normalize_invite_or_id(value: str) -> str:
    # Keep raw; actual parsing done where needed.
    return value.strip()
