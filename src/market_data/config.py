from pathlib import Path
from datetime import datetime, timedelta


DEFAULT_START_DATE = "2025-01-01"


def get_end_date():
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def get_output_dir() -> Path:
    output_dir = get_project_root() / "output"
    output_dir.mkdir(exist_ok=True)
    return output_dir
