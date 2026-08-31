from src.pipeline import bronze_2_silver, landing_2_bronze, silver_2_gold
from src.pipeline.db import get_bronze_conn, get_gold_conn, get_meta_conn, get_silver_conn
from tests.conftest import write_csv

HEADER = [
    "transaction_id", "order_datetime_utc", "ingested_at_utc", "country", "source_system",
    "source_scenario", "product_code", "product_name", "product_category", "brand",
    "sales_channel", "quantity", "unit_price", "currency", "discount_pct",
    "gross_amount", "net_amount",
]


def _run_batch(isolated_env, source_id, filename, rows):
    landing_dir = isolated_env["landing_dir"]
    csv_path = landing_dir / str(source_id) / filename
    write_csv(csv_path, HEADER, rows)

    meta_conn = get_meta_conn()
    bronze_conn = get_bronze_conn()
    try:
        landing_2_bronze.ingest_file(meta_conn, bronze_conn, source_id, source_id, csv_path)
    finally:
        bronze_conn.close()

    silver_conn = get_silver_conn()
    try:
        bronze_2_silver.merge(meta_conn, silver_conn, source_id, source_id)
    finally:
        silver_conn.close()
        meta_conn.close()


def test_recompute_all_kpis_from_silver_sales(isolated_env):
    _run_batch(isolated_env, 1, "input.csv", [
        ["TXN-1", "2026-08-25 09:00:00", "2026-08-25 09:01:00", "IT", "SAP S/4HANA", "A",
         "P100", "Product100", "Prescription", "BrandX", "Pharmacy/B2B",
         2, "10.00", "EUR", "0.0", "20.00", "20.00"],
        ["TXN-2", "2026-08-25 10:00:00", "2026-08-25 10:01:00", "IT", "SAP S/4HANA", "A",
         "P100", "Product100", "Prescription", "BrandX", "Pharmacy/B2B",
         1, "10.00", "EUR", "0.0", "10.00", "10.00"],
        ["TXN-3", "2026-08-25 11:00:00", "2026-08-25 11:01:00", "FR", "SAP S/4HANA", "A",
         "P200", "Product200", "OTC", "BrandY", "E-commerce",
         1, "50.00", "EUR", "0.0", "50.00", "50.00"],
    ])

    meta_conn = get_meta_conn()
    gold_conn = get_gold_conn()
    try:
        silver_2_gold.recompute_all_kpis(meta_conn, gold_conn)

        by_country = {r["country"]: r["net_amount_eur"] for r in gold_conn.execute("SELECT * FROM kpi_sales_by_country_day")}
        assert by_country == {"IT": 30.0, "FR": 50.0}

        by_category = {r["product_category"]: r["net_amount_eur"] for r in gold_conn.execute("SELECT * FROM kpi_sales_by_category_day")}
        assert by_category == {"Prescription": 30.0, "OTC": 50.0}

        by_channel = {r["sales_channel"]: r["net_amount_eur"] for r in gold_conn.execute("SELECT * FROM kpi_sales_by_channel_day")}
        assert by_channel == {"Pharmacy/B2B": 30.0, "E-commerce": 50.0}

        top_products = gold_conn.execute("SELECT * FROM kpi_top_products").fetchall()
        assert [r["product_code"] for r in top_products] == ["P200", "P100"]
        assert top_products[0]["net_amount_eur"] == 50.0
        assert top_products[1]["net_amount_eur"] == 30.0
        assert top_products[1]["transactions"] == 2

        overall = gold_conn.execute("SELECT * FROM kpi_sales_overall_day").fetchall()
        assert len(overall) == 1
        assert overall[0]["order_date"] == "2026-08-25"
        assert overall[0]["net_amount_eur"] == 80.0
        assert overall[0]["transactions"] == 3
        assert overall[0]["avg_order_value_eur"] == 26.67
        assert overall[0]["total_quantity"] == 4
    finally:
        meta_conn.close()
        gold_conn.close()


def test_recompute_all_kpis_is_a_full_refresh(isolated_env):
    _run_batch(isolated_env, 1, "input.csv", [
        ["TXN-1", "2026-08-25 09:00:00", "2026-08-25 09:01:00", "IT", "SAP S/4HANA", "A",
         "P100", "Product100", "Prescription", "BrandX", "Pharmacy/B2B",
         1, "10.00", "EUR", "0.0", "10.00", "10.00"],
    ])
    meta_conn = get_meta_conn()
    gold_conn = get_gold_conn()
    try:
        silver_2_gold.recompute_all_kpis(meta_conn, gold_conn)
    finally:
        meta_conn.close()
        gold_conn.close()

    _run_batch(isolated_env, 2, "input2.csv", [
        ["TXN-2", "2026-08-26 09:00:00", "2026-08-26 09:01:00", "fr", "Proprietary ERP", "b",
         "P100", "Product100", "Prescription", "BrandX", "Pharmacy/B2B",
         1, "30.00", "EUR", "0.0", "30.00", "30.00"],
    ])
    meta_conn = get_meta_conn()
    gold_conn = get_gold_conn()
    try:
        silver_2_gold.recompute_all_kpis(meta_conn, gold_conn)
        by_country = {r["country"]: r["net_amount_eur"] for r in gold_conn.execute("SELECT * FROM kpi_sales_by_country_day")}
        assert by_country == {"IT": 10.0, "FR": 30.0}
    finally:
        meta_conn.close()
        gold_conn.close()
