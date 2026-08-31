"""Helper di connessione SQLite per i livelli meta/bronze/silver/gold.

Ogni livello vive nel suo file .db separato; i livelli a valle si collegano a quelli
a monte con ATTACH DATABASE, cosi' silver puo' leggere "bronze.sales_raw" e gold puo'
leggere "silver.sales" con la sintassi a schema puntato richiesta dal progetto.
"""
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
LANDING_DIR = BASE_DIR / "landing"

META_DB = DATA_DIR / "meta.db"
BRONZE_DB = DATA_DIR / "bronze.db"
SILVER_DB = DATA_DIR / "silver.db"
GOLD_DB = DATA_DIR / "gold.db"


def _connect(path: Path) -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def get_meta_conn() -> sqlite3.Connection:
    return _connect(META_DB)


def get_bronze_conn() -> sqlite3.Connection:
    return _connect(BRONZE_DB)


def get_silver_conn(attach_bronze: bool = True) -> sqlite3.Connection:
    conn = _connect(SILVER_DB)
    if attach_bronze:
        conn.execute("ATTACH DATABASE ? AS bronze", (str(BRONZE_DB),))
    return conn


def get_gold_conn(attach_silver: bool = True) -> sqlite3.Connection:
    conn = _connect(GOLD_DB)
    if attach_silver:
        conn.execute("ATTACH DATABASE ? AS silver", (str(SILVER_DB),))
    return conn
