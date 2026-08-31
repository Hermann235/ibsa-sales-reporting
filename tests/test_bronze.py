from pathlib import Path

from src.pipeline import landing_2_bronze
from src.pipeline.db import get_bronze_conn, get_meta_conn
from tests.conftest import write_csv

HEADER = [
    "transaction_id", "order_datetime_utc", "ingested_at_utc", "country", "source_system",
    "source_scenario", "product_code", "product_name", "product_category", "brand",
    "sales_channel", "quantity", "unit_price", "currency", "discount_pct",
    "gross_amount", "net_amount",
]


def test_ingest_file_appends_raw_rows_with_technical_columns(isolated_env):
    landing_dir = isolated_env["landing_dir"]
    csv_path = landing_dir / "1" / "input.csv"
    write_csv(csv_path, HEADER, [
        ["TXN-1", "2026-08-25 09:00:00", "2026-08-25 09:01:00", "CH", "SAP S/4HANA", "A",
         "MED-001", "Medora Cardio 10mg", "Prescription", "Medora", "Pharmacy/B2B",
         2, "45.00", "CHF", "0.0", "90.00", "90.00"],
        ["TXN-2", "2026-08-25 10:00:00", "2026-08-25 10:01:00", "FR", "SAP S/4HANA", "A",
         "VIT-001", "Vitalis C1000", "Supplement", "Vitalis", "E-commerce",
         1, "22.50", "EUR", "0.0", "22.50", "22.50"],
    ])

    meta_conn = get_meta_conn()
    bronze_conn = get_bronze_conn()
    try:
        rows_inserted = landing_2_bronze.ingest_file(meta_conn, bronze_conn, source_id=1, batch_id=1, csv_path=csv_path)
        assert rows_inserted == 2

        rows = bronze_conn.execute("SELECT * FROM sales_raw ORDER BY transaction_id").fetchall()
        assert len(rows) == 2
        assert rows[0]["transaction_id"] == "TXN-1"
        assert rows[0]["country"] == "CH"
        assert rows[0]["_batch_id"] == 1
        assert rows[0]["_source_file"] == str(Path("1") / "input.csv")
        assert rows[0]["_ingestion_ts"] is not None
    finally:
        meta_conn.close()
        bronze_conn.close()


def test_ingest_file_raises_for_unknown_source(isolated_env):
    landing_dir = isolated_env["landing_dir"]
    csv_path = landing_dir / "99" / "input.csv"
    write_csv(csv_path, HEADER, [
        ["TXN-1", "2026-08-25 09:00:00", "2026-08-25 09:01:00", "CH", "SAP S/4HANA", "A",
         "MED-001", "Medora Cardio 10mg", "Prescription", "Medora", "Pharmacy/B2B",
         1, "45.00", "CHF", "0.0", "45.00", "45.00"],
    ])

    meta_conn = get_meta_conn()
    bronze_conn = get_bronze_conn()
    try:
        try:
            landing_2_bronze.ingest_file(meta_conn, bronze_conn, source_id=99, batch_id=1, csv_path=csv_path)
            assert False, "doveva sollevare ValueError per source_id non configurato"
        except ValueError:
            pass
    finally:
        meta_conn.close()
        bronze_conn.close()
