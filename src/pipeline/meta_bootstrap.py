"""Crea e popola (in modo idempotente) le tabelle di configurazione e controllo.

Bronze e silver non contengono nessuna logica hard-coded per sorgente: tutto cio' che
sanno su "quali file leggere", "che schema hanno" e "come mappare/deduplicare/convertire"
viene letto da queste tabelle, che vivono in meta.db. Il mapping bronze->silver (schema,
tipi, chiave primaria, regole di trasformazione) e' a sua volta letto da un file YAML
esterno (sales_config.yml, il "data contract"), modificabile a mano senza toccare il
codice.
"""
import sys
from pathlib import Path

import yaml

if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.pipeline.db import BASE_DIR, get_meta_conn

SALES_CONFIG_PATH = BASE_DIR / "sales_config.yml"

# le sorgenti sono 5, una per country (Italia/Svizzera/Cina/Germania/Inghilterra);
# ognuna ricorre identica in ogni cartella di landing (1, 2, 3, 4...), che rappresenta
# solo l'ondata di arrivo del batch, non la country. Lo schema/mapping di ogni sorgente
# e' identico e viene letto da sales_config.yml, con eventuali eccezioni per singola
# sorgente elencate li' sotto "source_overrides" (es. la Cina manda la categoria come
# codice abbreviato).
SOURCES = [
    (1, "Italia", "italia.csv"),
    (2, "Svizzera", "svizzera.csv"),
    (3, "Cina", "cina.csv"),
    (4, "Germania", "germania.csv"),
    (5, "Inghilterra", "inghilterra.csv"),
]
SOURCE_IDS = [source_id for source_id, _, _ in SOURCES]

FX_RATES_TO_EUR = {
    "EUR": 1.0,
    "CHF": 1.05,
    "CNY": 0.13,
    "GBP": 1.17,
}

DDL = """
CREATE TABLE IF NOT EXISTS cfg_source (
    source_id INTEGER PRIMARY KEY,
    source_name TEXT NOT NULL,
    file_pattern TEXT NOT NULL,
    target_bronze_table TEXT NOT NULL,
    delimiter TEXT NOT NULL DEFAULT ',',
    has_header INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS cfg_bronze_column (
    source_id INTEGER NOT NULL,
    column_name TEXT NOT NULL,
    column_type TEXT NOT NULL,
    ordinal_position INTEGER NOT NULL,
    is_required INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (source_id, column_name)
);

CREATE TABLE IF NOT EXISTS cfg_silver_mapping (
    source_id INTEGER NOT NULL,
    source_column TEXT NOT NULL,
    target_column TEXT NOT NULL,
    target_data_type TEXT NOT NULL DEFAULT 'TEXT',
    transform_rule TEXT NOT NULL DEFAULT 'none',
    PRIMARY KEY (source_id, source_column)
);

CREATE TABLE IF NOT EXISTS cfg_dedup_key (
    target_table TEXT PRIMARY KEY,
    key_columns TEXT NOT NULL,
    order_by_column TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cfg_fx_rate (
    currency TEXT PRIMARY KEY,
    rate_to_eur REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS cfg_kpi (
    kpi_id INTEGER PRIMARY KEY,
    kpi_name TEXT NOT NULL,
    target_gold_table TEXT NOT NULL,
    sql_definition TEXT NOT NULL,
    partition_column TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ctl_batch_log (
    batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER NOT NULL,
    landing_dir TEXT NOT NULL,
    source_file TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    rows_bronze INTEGER,
    rows_silver INTEGER,
    processed_at TEXT
);

CREATE TABLE IF NOT EXISTS ctl_country_watermark (
    country TEXT PRIMARY KEY,
    last_batch_id INTEGER,
    last_event_ts TEXT,
    last_updated_at TEXT
);
"""


def _load_sales_config() -> dict:
    with open(SALES_CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def bootstrap() -> None:
    contract = _load_sales_config()
    columns = contract["columns"]
    overrides = {
        (override["source_id"], override["column"]): override["transform"]
        for override in contract.get("source_overrides", [])
    }
    key_columns = [c["source_column"] for c in columns if c.get("primary_key")]

    conn = get_meta_conn()
    try:
        conn.executescript(DDL)

        bronze_table = columns[0]["source_table"]
        for source_id, source_name, file_pattern in SOURCES:
            conn.execute(
                """INSERT OR IGNORE INTO cfg_source
                   (source_id, source_name, file_pattern, target_bronze_table, delimiter, has_header)
                   VALUES (?, ?, ?, ?, ',', 1)""",
                (source_id, source_name, file_pattern, bronze_table),
            )

            for position, col in enumerate(columns, start=1):
                conn.execute(
                    """INSERT OR IGNORE INTO cfg_bronze_column
                       (source_id, column_name, column_type, ordinal_position, is_required)
                       VALUES (?, ?, 'TEXT', ?, 1)""",
                    (source_id, col["source_column"], position),
                )

            for col in columns:
                transform = overrides.get((source_id, col["source_column"]), col["transform"])
                conn.execute(
                    """INSERT OR IGNORE INTO cfg_silver_mapping
                       (source_id, source_column, target_column, target_data_type, transform_rule)
                       VALUES (?, ?, ?, ?, ?)""",
                    (source_id, col["source_column"], col["target_column"], col["data_type"], transform),
                )

        silver_table = columns[0]["target_table"]
        conn.execute(
            """INSERT OR IGNORE INTO cfg_dedup_key (target_table, key_columns, order_by_column)
               VALUES (?, ?, ?)""",
            (silver_table, ",".join(key_columns), contract["dedup_order_by"]),
        )

        for currency, rate in FX_RATES_TO_EUR.items():
            conn.execute(
                "INSERT OR IGNORE INTO cfg_fx_rate (currency, rate_to_eur) VALUES (?, ?)",
                (currency, rate),
            )

        kpis = [
            (1, "sales_by_country_day", "kpi_sales_by_country_day",
             "SELECT DATE(order_datetime_utc) AS order_date, country, "
             "ROUND(SUM(net_amount_eur), 2) AS net_amount_eur, "
             "COUNT(DISTINCT transaction_id) AS transactions "
             "FROM silver.sales GROUP BY order_date, country ORDER BY order_date, country",
             "order_date"),
            (2, "sales_by_category_day", "kpi_sales_by_category_day",
             "SELECT DATE(order_datetime_utc) AS order_date, product_category, "
             "ROUND(SUM(net_amount_eur), 2) AS net_amount_eur, "
             "COUNT(DISTINCT transaction_id) AS transactions "
             "FROM silver.sales GROUP BY order_date, product_category ORDER BY order_date, product_category",
             "order_date"),
            (3, "sales_by_channel_day", "kpi_sales_by_channel_day",
             "SELECT DATE(order_datetime_utc) AS order_date, sales_channel, "
             "ROUND(SUM(net_amount_eur), 2) AS net_amount_eur, "
             "COUNT(DISTINCT transaction_id) AS transactions "
             "FROM silver.sales GROUP BY order_date, sales_channel ORDER BY order_date, sales_channel",
             "order_date"),
            (4, "top_products", "kpi_top_products",
             "SELECT product_code, product_name, product_category, "
             "ROUND(SUM(net_amount_eur), 2) AS net_amount_eur, "
             "COUNT(DISTINCT transaction_id) AS transactions "
             "FROM silver.sales GROUP BY product_code, product_name, product_category "
             "ORDER BY net_amount_eur DESC",
             "product_code"),
            (5, "sales_overall_day", "kpi_sales_overall_day",
             "SELECT DATE(order_datetime_utc) AS order_date, "
             "ROUND(SUM(net_amount_eur), 2) AS net_amount_eur, "
             "COUNT(DISTINCT transaction_id) AS transactions, "
             "ROUND(SUM(net_amount_eur) * 1.0 / COUNT(DISTINCT transaction_id), 2) AS avg_order_value_eur, "
             "SUM(quantity) AS total_quantity "
             "FROM silver.sales GROUP BY order_date ORDER BY order_date",
             "order_date"),
        ]
        for kpi_id, kpi_name, target_table, sql_definition, partition_column in kpis:
            conn.execute(
                """INSERT OR IGNORE INTO cfg_kpi
                   (kpi_id, kpi_name, target_gold_table, sql_definition, partition_column)
                   VALUES (?, ?, ?, ?, ?)""",
                (kpi_id, kpi_name, target_table, sql_definition, partition_column),
            )

        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    bootstrap()
    print("meta.db pronto con le tabelle di configurazione e controllo.")
