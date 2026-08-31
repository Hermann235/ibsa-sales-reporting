"""API REST di sola lettura su SQLite: nessuna scrittura lato frontend."""
import sqlite3

from flask import Blueprint, jsonify

from src.pipeline.db import get_gold_conn, get_meta_conn

api_bp = Blueprint("api", __name__, url_prefix="/api")


def _rows_or_empty(conn: sqlite3.Connection, query: str, params: tuple = ()) -> list:
    try:
        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]
    except sqlite3.OperationalError:
        # la tabella non esiste ancora: nessun batch e' stato processato finora
        return []


@api_bp.route("/kpis")
def kpis():
    conn = get_gold_conn(attach_silver=False)
    try:
        return jsonify({
            "sales_overall_day": _rows_or_empty(conn, "SELECT * FROM kpi_sales_overall_day"),
            "sales_by_country_day": _rows_or_empty(conn, "SELECT * FROM kpi_sales_by_country_day"),
            "sales_by_category_day": _rows_or_empty(conn, "SELECT * FROM kpi_sales_by_category_day"),
            "sales_by_channel_day": _rows_or_empty(conn, "SELECT * FROM kpi_sales_by_channel_day"),
            "top_products": _rows_or_empty(conn, "SELECT * FROM kpi_top_products"),
        })
    finally:
        conn.close()


@api_bp.route("/countries/status")
def countries_status():
    conn = get_meta_conn()
    try:
        rows = _rows_or_empty(
            conn,
            "SELECT country, last_batch_id, last_event_ts, last_updated_at "
            "FROM ctl_country_watermark ORDER BY country",
        )
        return jsonify(rows)
    finally:
        conn.close()


@api_bp.route("/batches")
def batches():
    conn = get_meta_conn()
    try:
        rows = _rows_or_empty(
            conn,
            "SELECT batch_id, source_id, landing_dir, source_file, status, "
            "rows_bronze, rows_silver, processed_at "
            "FROM ctl_batch_log ORDER BY batch_id DESC",
        )
        return jsonify(rows)
    finally:
        conn.close()
