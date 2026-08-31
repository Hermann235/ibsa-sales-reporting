"""Bronze -> Silver: mapping/normalizzazione + dedup, guidati da cfg_silver_mapping / cfg_dedup_key.

Produce silver.sales con uno schema canonico unico indipendente da come la source
rappresentava i dati (es. product_category come etichetta estesa o come codice
abbreviato). Converte gli importi in EUR usando cfg_fx_rate (tassi statici, demo)
cosi' i KPI gold possono sommare vendite multi-valuta. Aggiorna anche il watermark
per country (l'ultimo evento business e l'ultimo aggiornamento ricevuti).

La scrittura in silver.sales avviene con un delete + insert esplicito sulle chiavi
del batch corrente (mai un upsert implicito): solo bronze puo' contenere righe
duplicate, silver deve sempre restare deduplicato per cfg_dedup_key.
"""
import sqlite3
from datetime import datetime

CATEGORY_CODE_TO_LABEL = {
    "RX": "Prescription",
    "OTC": "OTC",
    "MD": "Medical Device",
    "AES": "Aesthetic",
    "SUP": "Supplement",
}

TRANSFORMS = {
    "none": lambda v: v,
    "upper": lambda v: v.upper() if v is not None else v,
    "cast_int": lambda v: int(v),
    "cast_float": lambda v: float(v),
    "category_code_to_label": lambda v: CATEGORY_CODE_TO_LABEL.get(v, v) if v is not None else v,
}

TARGET_TABLE = "sales"


def _get_mapping(meta_conn: sqlite3.Connection, source_id: int):
    rows = meta_conn.execute(
        """SELECT source_column, target_column, transform_rule
           FROM cfg_silver_mapping WHERE source_id = ?""",
        (source_id,),
    ).fetchall()
    if not rows:
        raise ValueError(f"nessuna cfg_silver_mapping per source_id={source_id}")
    return rows


def _get_dedup_key(meta_conn: sqlite3.Connection):
    row = meta_conn.execute(
        "SELECT key_columns, order_by_column FROM cfg_dedup_key WHERE target_table = ?",
        (TARGET_TABLE,),
    ).fetchone()
    if row is None:
        raise ValueError(f"nessuna cfg_dedup_key per target_table={TARGET_TABLE}")
    return row["key_columns"].split(","), row["order_by_column"]


def _get_fx_rates(meta_conn: sqlite3.Connection) -> dict:
    rows = meta_conn.execute("SELECT currency, rate_to_eur FROM cfg_fx_rate").fetchall()
    return {row["currency"]: row["rate_to_eur"] for row in rows}


def _get_target_schema(meta_conn: sqlite3.Connection):
    """Schema di silver.sales (colonna, tipo) cosi' com'e' definito nel data contract
    (sales_config.yml), letto da cfg_silver_mapping. Il mapping e' identico per ogni
    source_id, quindi basta leggerlo da una qualunque sorgente configurata."""
    any_source_id = meta_conn.execute("SELECT MIN(source_id) FROM cfg_silver_mapping").fetchone()[0]
    rows = meta_conn.execute(
        """SELECT target_column, target_data_type FROM cfg_silver_mapping
           WHERE source_id = ? ORDER BY rowid""",
        (any_source_id,),
    ).fetchall()
    return [(row["target_column"], row["target_data_type"]) for row in rows]


def _ensure_sales_table(silver_conn: sqlite3.Connection, key_columns, target_schema) -> None:
    pk = ", ".join(key_columns)
    mapped_defs = ", ".join(f'"{name}" {data_type}' for name, data_type in target_schema)
    silver_conn.execute(
        f"CREATE TABLE IF NOT EXISTS {TARGET_TABLE} ("
        f"{mapped_defs}, "
        "gross_amount_eur REAL, net_amount_eur REAL, "
        "_batch_id INTEGER, _ingestion_ts TEXT, "
        f"PRIMARY KEY ({pk}))"
    )


def merge(meta_conn: sqlite3.Connection, silver_conn: sqlite3.Connection, source_id: int, batch_id: int):
    """Applica mapping + conversione FX + dedup sul batch appena arrivato in bronze e scrive in
    silver.sales con un delete + insert esplicito sulle chiavi del batch (mai un upsert implicito).

    Ritorna (righe scritte in silver, dict country -> max order_datetime_utc del batch,
    dict partition_column -> valori toccati dal batch) da usare rispettivamente per aggiornare
    il watermark per country e per far ricalcolare a gold solo le partizioni interessate.
    """
    mapping_rows = _get_mapping(meta_conn, source_id)
    key_columns, order_by_column = _get_dedup_key(meta_conn)
    fx_rates = _get_fx_rates(meta_conn)
    target_schema = _get_target_schema(meta_conn)
    _ensure_sales_table(silver_conn, key_columns, target_schema)

    bronze_table = meta_conn.execute(
        "SELECT target_bronze_table FROM cfg_source WHERE source_id = ?", (source_id,)
    ).fetchone()["target_bronze_table"]

    raw_rows = silver_conn.execute(
        f'SELECT * FROM bronze."{bronze_table}" WHERE _batch_id = ?', (batch_id,)
    ).fetchall()

    transformed = []
    for raw in raw_rows:
        record = {}
        for mapping in mapping_rows:
            value = raw[mapping["source_column"]]
            record[mapping["target_column"]] = TRANSFORMS[mapping["transform_rule"]](value)
        rate = fx_rates.get(record["currency"], 1.0)
        record["gross_amount_eur"] = round(record["gross_amount"] * rate, 2)
        record["net_amount_eur"] = round(record["net_amount"] * rate, 2)
        record["_batch_id"] = raw["_batch_id"]
        record["_ingestion_ts"] = raw["_ingestion_ts"]
        transformed.append(record)

    # dedup: ordina per la colonna di controllo (piu' order_datetime_utc come tie-break) cosi'
    # l'ultima occorrenza scritta nel dict e' quella che "vince" per ogni chiave.
    transformed.sort(key=lambda r: (r.get(order_by_column, ""), r.get("order_datetime_utc", "")))
    deduped = {}
    for record in transformed:
        key = tuple(record[col] for col in key_columns)
        deduped[key] = record

    country_max_ts = {}
    affected = {"order_date": set(), "product_code": set()}
    if deduped:
        columns = list(next(iter(deduped.values())).keys())
        quoted_cols = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join(["?"] * len(columns))

        # delete + insert esplicito sulle chiavi del batch corrente, mai un upsert implicito:
        # solo bronze puo' avere righe duplicate, qui la stessa chiave viene sempre rimossa e
        # riscritta da zero con l'ultima versione vincente.
        quoted_keys = ", ".join(f'"{c}"' for c in key_columns)
        key_placeholders = ", ".join(
            "(" + ", ".join(["?"] * len(key_columns)) + ")" for _ in deduped
        )
        silver_conn.execute(
            f"DELETE FROM {TARGET_TABLE} WHERE ({quoted_keys}) IN ({key_placeholders})",
            [value for key in deduped for value in key],
        )
        silver_conn.executemany(
            f"INSERT INTO {TARGET_TABLE} ({quoted_cols}) VALUES ({placeholders})",
            [[record[c] for c in columns] for record in deduped.values()],
        )
        silver_conn.commit()

        for record in deduped.values():
            country = record["country"]
            if country not in country_max_ts or record["order_datetime_utc"] > country_max_ts[country]:
                country_max_ts[country] = record["order_datetime_utc"]
            affected["order_date"].add(record["order_datetime_utc"][:10])
            affected["product_code"].add(record["product_code"])

    affected = {k: sorted(v) for k, v in affected.items()}
    return len(deduped), country_max_ts, affected


def update_country_watermark(meta_conn: sqlite3.Connection, batch_id: int, country_event_ts: dict) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    for country, event_ts in country_event_ts.items():
        existing = meta_conn.execute(
            "SELECT last_event_ts FROM ctl_country_watermark WHERE country = ?", (country,)
        ).fetchone()
        if existing is None or event_ts > existing["last_event_ts"]:
            meta_conn.execute(
                """INSERT INTO ctl_country_watermark (country, last_batch_id, last_event_ts, last_updated_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(country) DO UPDATE SET
                     last_batch_id = excluded.last_batch_id,
                     last_event_ts = excluded.last_event_ts,
                     last_updated_at = excluded.last_updated_at""",
                (country, batch_id, event_ts, now),
            )
        else:
            meta_conn.execute(
                "UPDATE ctl_country_watermark SET last_batch_id = ?, last_updated_at = ? WHERE country = ?",
                (batch_id, now, country),
            )
    meta_conn.commit()
