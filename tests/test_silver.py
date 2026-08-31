from src.pipeline import bronze_2_silver, landing_2_bronze
from src.pipeline.db import get_bronze_conn, get_meta_conn, get_silver_conn
from tests.conftest import write_csv

HEADER = [
    "transaction_id", "order_datetime_utc", "ingested_at_utc", "country", "source_system",
    "source_scenario", "product_code", "product_name", "product_category", "brand",
    "sales_channel", "quantity", "unit_price", "currency", "discount_pct",
    "gross_amount", "net_amount",
]


def _ingest(isolated_env, source_id, filename, rows):
    landing_dir = isolated_env["landing_dir"]
    csv_path = landing_dir / str(source_id) / filename
    write_csv(csv_path, HEADER, rows)

    meta_conn = get_meta_conn()
    bronze_conn = get_bronze_conn()
    try:
        landing_2_bronze.ingest_file(meta_conn, bronze_conn, source_id=source_id, batch_id=source_id, csv_path=csv_path)
    finally:
        meta_conn.close()
        bronze_conn.close()


def test_merge_normalizes_category_code_and_converts_to_eur(isolated_env):
    _ingest(isolated_env, source_id=3, filename="input3.csv", rows=[
        ["TXN-1", "2026-08-27 08:00:00", "2026-08-27 08:05:00", "cn", "Other ERP", "c",
         "MED-001", "Medora Cardio 10mg", "RX", "Medora", "Pharmacy/B2B",
         2, "10.00", "cny", "0.0", "20.00", "20.00"],
    ])

    meta_conn = get_meta_conn()
    silver_conn = get_silver_conn()
    try:
        rows_written, country_ts, _ = bronze_2_silver.merge(meta_conn, silver_conn, source_id=3, batch_id=3)
        assert rows_written == 1
        assert country_ts == {"CN": "2026-08-27 08:00:00"}

        row = silver_conn.execute("SELECT * FROM sales WHERE transaction_id = 'TXN-1'").fetchone()
        assert row["country"] == "CN"
        assert row["source_scenario"] == "C"
        assert row["currency"] == "CNY"
        assert row["product_category"] == "Prescription"
        assert row["net_amount_eur"] == 2.6  # 20.00 * tasso CNY->EUR (0.13)
    finally:
        meta_conn.close()
        silver_conn.close()


def test_merge_dedups_within_batch_keeping_latest_ingested(isolated_env):
    _ingest(isolated_env, source_id=4, filename="input4.csv", rows=[
        ["TXN-9", "2026-08-28 10:00:00", "2026-08-28 10:01:00", "CH", "SaaS Platform", "D",
         "DER-001", "Dermacare GlowSerum", "Aesthetic", "Dermacare", "E-commerce",
         1, "34.00", "CHF", "0.0", "34.00", "34.00"],
        ["TXN-9", "2026-08-28 10:00:00", "2026-08-28 10:05:00", "CH", "SaaS Platform", "D",
         "DER-001", "Dermacare GlowSerum", "Aesthetic", "Dermacare", "E-commerce",
         5, "34.00", "CHF", "0.0", "170.00", "170.00"],
    ])

    meta_conn = get_meta_conn()
    silver_conn = get_silver_conn()
    try:
        rows_written, _, _ = bronze_2_silver.merge(meta_conn, silver_conn, source_id=4, batch_id=4)
        assert rows_written == 1

        rows = silver_conn.execute("SELECT * FROM sales WHERE transaction_id = 'TXN-9'").fetchall()
        assert len(rows) == 1
        assert rows[0]["quantity"] == 5
    finally:
        meta_conn.close()
        silver_conn.close()


def test_merge_upserts_across_batches_latest_wins(isolated_env):
    _ingest(isolated_env, source_id=1, filename="input.csv", rows=[
        ["TXN-5", "2026-08-25 09:00:00", "2026-08-25 09:01:00", "CH", "SAP S/4HANA", "A",
         "MED-001", "Medora Cardio 10mg", "Prescription", "Medora", "Pharmacy/B2B",
         1, "45.00", "CHF", "0.0", "45.00", "45.00"],
    ])
    meta_conn = get_meta_conn()
    silver_conn = get_silver_conn()
    try:
        bronze_2_silver.merge(meta_conn, silver_conn, source_id=1, batch_id=1)
    finally:
        meta_conn.close()
        silver_conn.close()

    _ingest(isolated_env, source_id=4, filename="input4.csv", rows=[
        ["TXN-5", "2026-08-28 10:30:00", "2026-08-28 10:35:00", "CH", "SaaS Platform", "D",
         "MED-001", "Medora Cardio 10mg", "Prescription", "Medora", "Pharmacy/B2B",
         5, "45.00", "CHF", "0.0", "225.00", "225.00"],
    ])
    meta_conn = get_meta_conn()
    silver_conn = get_silver_conn()
    try:
        bronze_2_silver.merge(meta_conn, silver_conn, source_id=4, batch_id=4)
        row = silver_conn.execute("SELECT * FROM sales WHERE transaction_id = 'TXN-5'").fetchone()
        assert row["quantity"] == 5
        assert row["_batch_id"] == 4
    finally:
        meta_conn.close()
        silver_conn.close()


def test_update_country_watermark_keeps_max_event_ts(isolated_env):
    meta_conn = get_meta_conn()
    try:
        bronze_2_silver.update_country_watermark(meta_conn, batch_id=1, country_event_ts={"IT": "2026-08-25 09:00:00"})
        bronze_2_silver.update_country_watermark(meta_conn, batch_id=2, country_event_ts={"IT": "2026-08-20 00:00:00"})

        row = meta_conn.execute("SELECT * FROM ctl_country_watermark WHERE country = 'IT'").fetchone()
        assert row["last_event_ts"] == "2026-08-25 09:00:00"
        assert row["last_batch_id"] == 2
    finally:
        meta_conn.close()
