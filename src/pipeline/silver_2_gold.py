"""Silver -> Gold: ricalcola i KPI leggendo la loro definizione SQL da cfg_kpi (config-driven).

Ogni KPI viene scritto con un delete + insert esplicito, sempre scoped alla
"partizione" del KPI (cfg_kpi.partition_column: 'order_date' per i KPI temporali,
'product_code' per il ranking globale dei top_products). Quando l'orchestratore
processa un batch, passa in 'affected' solo le partizioni toccate dal batch corrente
(le date/prodotti "piu' recenti"), cosi' il ricalcolo non tocca lo storico gia'
scritto. Se 'affected' non e' fornito (full refresh, usato da test/reset manuali),
le partizioni vengono derivate da tutto silver.sales.
"""
import sqlite3

_FULL_SCOPE_QUERIES = {
    "order_date": "SELECT DISTINCT DATE(order_datetime_utc) FROM silver.sales",
    "product_code": "SELECT DISTINCT product_code FROM silver.sales",
}


def _full_scope_values(gold_conn: sqlite3.Connection, partition_column: str) -> list:
    query = _FULL_SCOPE_QUERIES.get(partition_column)
    if query is None:
        raise ValueError(f"nessuna query di full-scope per partition_column={partition_column}")
    return [row[0] for row in gold_conn.execute(query).fetchall()]


def recompute_all_kpis(meta_conn: sqlite3.Connection, gold_conn: sqlite3.Connection, affected: dict = None) -> None:
    kpis = meta_conn.execute(
        "SELECT kpi_name, target_gold_table, sql_definition, partition_column FROM cfg_kpi"
    ).fetchall()
    if not kpis:
        raise ValueError("nessun KPI configurato in cfg_kpi")

    for kpi in kpis:
        table = kpi["target_gold_table"]
        partition_column = kpi["partition_column"]
        sql_definition = kpi["sql_definition"]

        values = _full_scope_values(gold_conn, partition_column) if affected is None else affected.get(partition_column, [])
        if not values:
            continue

        placeholders = ", ".join(["?"] * len(values))
        table_exists = gold_conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
        ).fetchone()

        if not table_exists:
            gold_conn.execute(
                f'CREATE TABLE "{table}" AS SELECT * FROM ({sql_definition}) '
                f'WHERE "{partition_column}" IN ({placeholders})',
                values,
            )
        else:
            gold_conn.execute(
                f'DELETE FROM "{table}" WHERE "{partition_column}" IN ({placeholders})', values
            )
            gold_conn.execute(
                f'INSERT INTO "{table}" SELECT * FROM ({sql_definition}) '
                f'WHERE "{partition_column}" IN ({placeholders})',
                values,
            )
    gold_conn.commit()
