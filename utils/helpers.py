import os
from datetime import datetime
from typing import List, Dict, Any


def get_timestamp() -> int:
    return int(datetime.now().timestamp())


def ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


def format_time(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')


def safe_str(value: Any, default: str = '') -> str:
    if value is None or value == '' or str(value).lower() in ('nan', 'none', 'null'):
        return default
    return str(value).strip().replace('\n', ' ').replace('\r', ' ')


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value) if value is not None else default
    except (ValueError, TypeError):
        return default
