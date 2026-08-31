"""Genera i CSV statici di landing (landing/1, landing/2, ...) usati dalla demo.

Ogni cartella numerata (1, 2, 3, 4) rappresenta un'ondata di arrivo e contiene un file
per ciascuna delle 5 country simulate (Italia, Svizzera, Cina, Germania, Inghilterra):
sistema sorgente, valuta e nome file cambiano per country, e la Cina manda la categoria
prodotto come codice abbreviato (RX/OTC/MD/AES/SUP) anziche' come etichetta estesa, per
dimostrare la normalizzazione fatta dal livello silver via cfg_silver_mapping. Il numero
di righe per (cartella, country) e' casuale tra 1000 e 2000 (seed fissato per
riproducibilita'). L'ultima country dell'ultima cartella include in piu' una transazione
duplicata (stesso transaction_id, ingested_at_utc diverso, per simulare una consegna
doppia dello stesso evento) e la Svizzera porta una correzione cross-batch di un ordine
arrivato nella prima cartella, per dimostrare la deduplica e l'upsert "latest wins" su
transaction_id.
"""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
LANDING_DIR = BASE_DIR / "landing"

SEED = 42
MIN_ROWS_PER_BATCH = 1000
MAX_ROWS_PER_BATCH = 2000
LANDING_DIRS = [1, 2, 3, 4]

HEADER = [
    "transaction_id",
    "order_datetime_utc",
    "ingested_at_utc",
    "country",
    "source_system",
    "source_scenario",
    "product_code",
    "product_name",
    "product_category",
    "brand",
    "sales_channel",
    "quantity",
    "unit_price",
    "currency",
    "discount_pct",
    "gross_amount",
    "net_amount",
]

# prezzo di riferimento in EUR: il prezzo nella valuta locale e' derivato da questo
# via cfg_fx_rate, cosi' il valore di ogni vendita resta paragonabile tra country
# pur essendo espresso in valute diverse.
PRODUCTS = [
    {"code": "MED-001", "name": "Medora Cardio 10mg", "category": "Prescription", "category_code": "RX", "brand": "Medora", "base_eur_price": 43.0},
    {"code": "MED-002", "name": "Medora Derm Cream", "category": "Prescription", "category_code": "RX", "brand": "Medora", "base_eur_price": 26.0},
    {"code": "VIT-001", "name": "Vitalis C1000", "category": "Supplement", "category_code": "SUP", "brand": "Vitalis", "base_eur_price": 21.0},
    {"code": "VIT-002", "name": "Vitalis Multivit", "category": "Supplement", "category_code": "SUP", "brand": "Vitalis", "base_eur_price": 17.0},
    {"code": "SAN-001", "name": "SanaPlus PainRelief", "category": "OTC", "category_code": "OTC", "brand": "SanaPlus", "base_eur_price": 9.5},
    {"code": "SAN-002", "name": "SanaPlus ColdFlu", "category": "OTC", "category_code": "OTC", "brand": "SanaPlus", "base_eur_price": 10.5},
    {"code": "DER-001", "name": "Dermacare GlowSerum", "category": "Aesthetic", "category_code": "AES", "brand": "Dermacare", "base_eur_price": 32.0},
    {"code": "DER-002", "name": "Dermacare LiftCream", "category": "Aesthetic", "category_code": "AES", "brand": "Dermacare", "base_eur_price": 37.0},
    {"code": "PUR-001", "name": "PureLife GlucoMonitor", "category": "Medical Device", "category_code": "MD", "brand": "PureLife", "base_eur_price": 85.0},
    {"code": "PUR-002", "name": "PureLife BP Monitor", "category": "Medical Device", "category_code": "MD", "brand": "PureLife", "base_eur_price": 90.0},
]
PRODUCTS_BY_CODE = {p["code"]: p for p in PRODUCTS}

CHANNELS = ["Pharmacy/B2B", "E-commerce", "D2C", "Wholesale"]
DISCOUNTS = [0.0, 0.0, 0.0, 0.05, 0.10, 0.15, 0.20]

FX_RATES_TO_EUR = {"EUR": 1.0, "CHF": 1.05, "CNY": 0.13, "GBP": 1.17}

# le 5 country simulate, per ora fisse: stesso set ripetuto identico in ogni cartella
# di landing, solo i dati (righe, timestamp) cambiano batch dopo batch.
COUNTRIES = [
    {"code": "IT", "file_name": "italia.csv", "currency": "EUR", "source_system": "Proprietary ERP", "scenario": "B"},
    {"code": "CH", "file_name": "svizzera.csv", "currency": "CHF", "source_system": "SAP S/4HANA", "scenario": "A"},
    {"code": "CN", "file_name": "cina.csv", "currency": "CNY", "source_system": "Other ERP", "scenario": "C", "use_category_code": True},
    {"code": "DE", "file_name": "germania.csv", "currency": "EUR", "source_system": "SaaS Platform", "scenario": "D"},
    {"code": "GB", "file_name": "inghilterra.csv", "currency": "GBP", "source_system": "Legacy Oracle EBS", "scenario": "E"},
]

# un giorno diverso per cartella, condiviso da tutte le country di quella cartella,
# cosi' ogni batch fa avanzare il watermark per country in modo visibile.
BATCH_DAY = {1: "2026-08-25", 2: "2026-08-26", 3: "2026-08-27", 4: "2026-08-28"}


def local_price(product, currency):
    return round(product["base_eur_price"] / FX_RATES_TO_EUR[currency], 2)


def row(transaction_id, order_dt, ingested_dt, country, source_system, scenario,
        product_code, channel, qty, unit_price, currency, discount_pct, use_category_code=False):
    product = PRODUCTS_BY_CODE[product_code]
    category = product["category_code"] if use_category_code else product["category"]
    gross = round(qty * unit_price, 2)
    net = round(gross * (1 - discount_pct), 2)
    return [
        transaction_id, order_dt, ingested_dt, country, source_system, scenario,
        product_code, product["name"], category, product["brand"], channel,
        qty, unit_price, currency, discount_pct, gross, net,
    ]


def _shift_timestamp(ts: str, minutes: int) -> str:
    return (datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")


def _random_timestamps(day: str):
    base = datetime.strptime(day, "%Y-%m-%d")
    order_dt = base + timedelta(seconds=random.randint(0, 24 * 3600 - 1))
    ingested_dt = order_dt + timedelta(minutes=random.randint(1, 6))
    return order_dt.strftime("%Y-%m-%d %H:%M:%S"), ingested_dt.strftime("%Y-%m-%d %H:%M:%S")


def generate_country_rows(country, landing_dir, n):
    rows = []
    day = BATCH_DAY[landing_dir]
    id_prefix = f"{country['code']}-{landing_dir}"
    for i in range(1, n + 1):
        transaction_id = f"TXN-{id_prefix}-{i:06d}"
        order_dt, ingested_dt = _random_timestamps(day)
        product = random.choice(PRODUCTS)
        channel = random.choice(CHANNELS)
        qty = random.randint(1, 12)
        discount = random.choice(DISCOUNTS)
        unit_price = local_price(product, country["currency"])
        rows.append(row(
            transaction_id, order_dt, ingested_dt, country["code"], country["source_system"], country["scenario"],
            product["code"], channel, qty, unit_price, country["currency"], discount,
            use_category_code=country.get("use_category_code", False),
        ))
    return rows


def build_batches():
    random.seed(SEED)
    batches = {}
    for landing_dir in LANDING_DIRS:
        country_files = {}
        for country in COUNTRIES:
            n = random.randint(MIN_ROWS_PER_BATCH, MAX_ROWS_PER_BATCH)
            country_files[country["file_name"]] = generate_country_rows(country, landing_dir, n)
        batches[landing_dir] = country_files

    # duplicato intra-batch: ultima riga generata per la Germania nell'ultima cartella,
    # consegnata una seconda volta con ingested_at_utc diverso -> dedup intra-batch
    germania_rows = batches[4]["germania.csv"]
    duplicate = list(germania_rows[-1])
    duplicate[2] = _shift_timestamp(duplicate[2], minutes=4)
    germania_rows.append(duplicate)

    # correzione cross-batch: la prima transazione della Svizzera nella prima cartella
    # arriva di nuovo nell'ultima cartella con quantita' corretta e timestamp piu'
    # recente -> upsert "latest wins" sullo stesso file logico (svizzera.csv)
    svizzera_country = next(c for c in COUNTRIES if c["code"] == "CH")
    batches[4]["svizzera.csv"].append(row(
        "TXN-CH-1-000001", "2026-08-28 10:30:00", "2026-08-28 10:35:00",
        "CH", svizzera_country["source_system"], svizzera_country["scenario"], "MED-001", "Pharmacy/B2B",
        5, local_price(PRODUCTS_BY_CODE["MED-001"], "CHF"), "CHF", 0.0,
    ))

    return batches


def main():
    LANDING_DIR.mkdir(exist_ok=True)
    for landing_dir, country_files in build_batches().items():
        batch_dir = LANDING_DIR / str(landing_dir)
        batch_dir.mkdir(exist_ok=True)
        for file_name, rows in country_files.items():
            csv_path = batch_dir / file_name
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(HEADER)
                writer.writerows(rows)
            print(f"scritto {csv_path} ({len(rows)} righe)")


if __name__ == "__main__":
    main()
