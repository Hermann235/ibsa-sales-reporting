"""Ingestion bronze: append-only, nessuna trasformazione di business.

Tutto cio' che questo modulo sa su una sorgente (dove sta il file, che colonne ha,
in che tabella bronze va scritto) viene letto dalle tabelle di configurazione
cfg_source / cfg_bronze_column in meta.db: non c'e' nessun riferimento hard-coded
a una source_id specifica.
"""
import csv
import sqlite3
from datetime import datetime
from pathlib import Path

from src.pipeline import db


def _get_source_config(meta_conn: sqlite3.Connection, source_id: int) -> sqlite3.Row:
    row = meta_conn.execute(
        "SELECT * FROM cfg_source WHERE source_id = ?", (source_id,)
    ).fetchone()
    if row is None:
        raise ValueError(f"nessuna cfg_source per source_id={source_id}")
    return row


def _get_columns(meta_conn: sqlite3.Connection, source_id: int):
    rows = meta_conn.execute(
        """SELECT column_name, column_type FROM cfg_bronze_column
           WHERE source_id = ? ORDER BY ordinal_position""",
        (source_id,),
    ).fetchall()
    if not rows:
        raise ValueError(f"nessuna cfg_bronze_column per source_id={source_id}")
    return [(row["column_name"], row["column_type"]) for row in rows]


def _ensure_table(bronze_conn: sqlite3.Connection, table_name: str, columns) -> None:
    column_defs = ", ".join(f'"{name}" {col_type}' for name, col_type in columns)
    bronze_conn.execute(
        f'CREATE TABLE IF NOT EXISTS "{table_name}" ('
        f"{column_defs}, "
        f"_source_id INTEGER, _batch_id INTEGER, _source_file TEXT, _ingestion_ts TEXT)"
    )


def ingest_file(
    meta_conn: sqlite3.Connection,
    bronze_conn: sqlite3.Connection,
    source_id: int,
    batch_id: int,
    csv_path: Path,
) -> int:
    """Carica un file CSV di landing in bronze cosi' com'e', guidato dalla config."""
    source_cfg = _get_source_config(meta_conn, source_id)
    columns = _get_columns(meta_conn, source_id)
    column_names = [name for name, _ in columns]
    table_name = source_cfg["target_bronze_table"]

    _ensure_table(bronze_conn, table_name, columns)

    ingestion_ts = datetime.now().isoformat(timespec="seconds")
    delimiter = source_cfg["delimiter"]
    source_file = str(Path(csv_path).relative_to(db.LANDING_DIR))

    rows_to_insert = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for raw_row in reader:
            values = [raw_row.get(col) for col in column_names]
            values += [source_id, batch_id, source_file, ingestion_ts]
            rows_to_insert.append(values)

    if rows_to_insert:
        quoted_cols = ", ".join(f'"{c}"' for c in column_names)
        placeholders = ", ".join(["?"] * (len(column_names) + 4))
        bronze_conn.executemany(
            f'INSERT INTO "{table_name}" '
            f"({quoted_cols}, _source_id, _batch_id, _source_file, _ingestion_ts) "
            f"VALUES ({placeholders})",
            rows_to_insert,
        )
        bronze_conn.commit()

    return len(rows_to_insert)
