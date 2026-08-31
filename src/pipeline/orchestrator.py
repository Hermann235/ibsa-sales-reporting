"""Orchestrazione del batch successivo: landing -> bronze -> silver -> gold.

Ogni cartella di landing (1, 2, 3, 4, ...) rappresenta un'ondata di arrivo e contiene
un file per ciascuna delle country configurate in cfg_source (Italia, Svizzera, Cina,
Germania, Inghilterra). Un batch e' quindi identificato dalla coppia (source_id,
landing_dir); l'avanzamento e' tracciato in ctl_batch_log per quella coppia, scandendo
le cartelle in ordine numerico e, dentro ciascuna, le country nell'ordine di cfg_source.

run_next_landing_dir() e' il punto di ingresso usato dallo scheduler: processa TUTTE
le country ancora mancanti della prossima cartella non completata in un colpo solo,
cosi' i KPI gold per una data (che si ri-aggregano sempre su tutto silver.sales per
quella data) arrivano gia' completi di tutte le country invece di popolarsi una country
alla volta nell'arco di piu' tick, il che farebbe apparire l'ultima data come parziale/
in calo finche' non arrivano anche le country mancanti. run_next_batch() resta
disponibile per processare una singola country alla volta (test/debug manuali).
"""
import sys
from datetime import datetime
from pathlib import Path

if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.pipeline import bronze_2_silver, landing_2_bronze, silver_2_gold
from src.pipeline import db
from src.pipeline.db import get_bronze_conn, get_gold_conn, get_meta_conn, get_silver_conn


def _landing_dirs():
    """Sottocartelle numeriche di landing/, in ordine di arrivo (1, 2, 3, ...)."""
    if not db.LANDING_DIR.exists():
        return []
    return sorted(
        (p.name for p in db.LANDING_DIR.iterdir() if p.is_dir()),
        key=lambda name: (int(name) if name.isdigit() else float("inf"), name),
    )


def _find_next_batch(meta_conn):
    """Prossima coppia (source_id, landing_dir) senza un batch 'done', scandendo le
    cartelle in ordine numerico e, dentro ciascuna, le country in ordine di cfg_source."""
    sources = meta_conn.execute("SELECT source_id FROM cfg_source ORDER BY source_id").fetchall()
    done_pairs = {
        (row["source_id"], row["landing_dir"])
        for row in meta_conn.execute(
            "SELECT DISTINCT source_id, landing_dir FROM ctl_batch_log WHERE status = 'done'"
        ).fetchall()
    }
    for landing_dir in _landing_dirs():
        for source in sources:
            pair = (source["source_id"], landing_dir)
            if pair not in done_pairs:
                return source["source_id"], landing_dir
    return None, None


def _find_csv_file(meta_conn, source_id: int, landing_dir: str):
    folder = db.LANDING_DIR / landing_dir
    if not folder.exists():
        return None
    file_pattern = meta_conn.execute(
        "SELECT file_pattern FROM cfg_source WHERE source_id = ?", (source_id,)
    ).fetchone()["file_pattern"]
    matches = sorted(folder.glob(file_pattern))
    return matches[0] if matches else None


def _process_batch(meta_conn, source_id: int, landing_dir: str) -> dict:
    """Esegue landing -> bronze -> silver -> gold per una singola coppia (source_id, landing_dir)."""
    csv_path = _find_csv_file(meta_conn, source_id, landing_dir)
    if csv_path is None:
        return {"status": "no_file_found", "source_id": source_id, "landing_dir": landing_dir}

    cursor = meta_conn.execute(
        """INSERT INTO ctl_batch_log (source_id, landing_dir, source_file, status)
           VALUES (?, ?, ?, 'pending')""",
        (source_id, landing_dir, csv_path.name),
    )
    meta_conn.commit()
    batch_id = cursor.lastrowid

    bronze_conn = get_bronze_conn()
    try:
        rows_bronze = landing_2_bronze.ingest_file(meta_conn, bronze_conn, source_id, batch_id, csv_path)
    finally:
        bronze_conn.close()

    silver_conn = get_silver_conn()
    try:
        rows_silver, country_event_ts, affected = bronze_2_silver.merge(meta_conn, silver_conn, source_id, batch_id)
    finally:
        silver_conn.close()
    bronze_2_silver.update_country_watermark(meta_conn, batch_id, country_event_ts)

    gold_conn = get_gold_conn()
    try:
        silver_2_gold.recompute_all_kpis(meta_conn, gold_conn, affected=affected)
    finally:
        gold_conn.close()

    meta_conn.execute(
        """UPDATE ctl_batch_log
           SET status = 'done', rows_bronze = ?, rows_silver = ?, processed_at = ?
           WHERE batch_id = ?""",
        (rows_bronze, rows_silver, datetime.now().isoformat(timespec="seconds"), batch_id),
    )
    meta_conn.commit()

    return {
        "status": "ok",
        "batch_id": batch_id,
        "source_id": source_id,
        "landing_dir": landing_dir,
        "rows_bronze": rows_bronze,
        "rows_silver": rows_silver,
        "countries_updated": sorted(country_event_ts.keys()),
    }


def run_next_batch() -> dict:
    """Processa la prossima coppia (source_id, landing_dir) disponibile (una country alla
    volta). Ritorna un dict con l'esito. Pensata per test/debug manuali granulari; lo
    scheduler usa invece run_next_landing_dir()."""
    meta_conn = get_meta_conn()
    try:
        source_id, landing_dir = _find_next_batch(meta_conn)
        if source_id is None:
            return {"status": "no_new_batch"}
        return _process_batch(meta_conn, source_id, landing_dir)
    finally:
        meta_conn.close()


def run_next_landing_dir() -> dict:
    """Processa in un solo colpo tutte le country ancora mancanti della prossima cartella
    di landing non completata. E' il punto di ingresso usato dallo scheduler: cosi' un
    singolo tick aggiorna sempre una data/cartella per intero (tutte le country insieme)
    invece di farla apparire parziale finche' non arrivano anche le country mancanti."""
    meta_conn = get_meta_conn()
    try:
        source_id, landing_dir = _find_next_batch(meta_conn)
        if source_id is None:
            return {"status": "no_new_batch"}

        target_landing_dir = landing_dir
        batches = []
        while source_id is not None and landing_dir == target_landing_dir:
            result = _process_batch(meta_conn, source_id, landing_dir)
            batches.append(result)
            if result["status"] != "ok":
                # niente riga 'done' scritta per questa coppia (es. file non ancora
                # arrivato): fermarsi qui, altrimenti _find_next_batch ritornerebbe
                # all'infinito la stessa coppia non completata.
                break
            source_id, landing_dir = _find_next_batch(meta_conn)

        countries_updated = sorted({c for b in batches for c in b.get("countries_updated", [])})
        return {
            "status": "ok",
            "landing_dir": target_landing_dir,
            "batches": batches,
            "countries_updated": countries_updated,
        }
    finally:
        meta_conn.close()


if __name__ == "__main__":
    from src.pipeline.meta_bootstrap import bootstrap

    bootstrap()
    print(run_next_landing_dir())
