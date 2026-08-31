import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.pipeline import db as db_module
from src.pipeline.meta_bootstrap import bootstrap


@pytest.fixture
def isolated_env(tmp_path, monkeypatch):
    """Reindirizza tutti i path di db.py verso una cartella temporanea per ogni test."""
    data_dir = tmp_path / "data"
    landing_dir = tmp_path / "landing"
    data_dir.mkdir()
    landing_dir.mkdir()

    monkeypatch.setattr(db_module, "DATA_DIR", data_dir)
    monkeypatch.setattr(db_module, "LANDING_DIR", landing_dir)
    monkeypatch.setattr(db_module, "META_DB", data_dir / "meta.db")
    monkeypatch.setattr(db_module, "BRONZE_DB", data_dir / "bronze.db")
    monkeypatch.setattr(db_module, "SILVER_DB", data_dir / "silver.db")
    monkeypatch.setattr(db_module, "GOLD_DB", data_dir / "gold.db")

    bootstrap()
    return {"data_dir": data_dir, "landing_dir": landing_dir}


def write_csv(path: Path, header, rows):
    import csv

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
