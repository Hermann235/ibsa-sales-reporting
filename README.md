# IBSA Sales Reporting Demo (Landing → Bronze → Silver → Gold, SQLite + Flask)

Demo locale di un'architettura medallion per la reportistica vendite IBSA. Nessuna
ingestion reale: i "chunk" arrivano come CSV statici già presenti in `landing/1`,
`landing/2`, ... e vengono processati uno alla volta da uno scheduler in-process ogni
30 secondi.

## Livelli

- **Landing** (`landing/<n>/<country>.csv`): file CSV statici, uno per country (Italia,
  Svizzera, Cina, Germania, Inghilterra) in ciascuna cartella numerata; ogni cartella
  rappresenta un'ondata di arrivo.
- **Bronze** (`data/bronze.db`, tabella `sales_raw`): ingestion raw, append-only, guidata
  dalle tabelle di configurazione `cfg_source` / `cfg_bronze_column` in `data/meta.db`.
  Solo il bronze puo' contenere righe duplicate (consegne doppie dello stesso evento).
- **Silver** (`data/silver.db`, tabella `sales`): mapping/normalizzazione country e dedup
  guidati da `cfg_silver_mapping` / `cfg_dedup_key`, scritti con un delete + insert
  esplicito sulle chiavi del batch corrente (mai un upsert implicito); aggiorna anche il
  watermark per country (`ctl_country_watermark`) con il timestamp dell'ultimo evento
  ricevuto.
- **Gold** (`data/gold.db`): KPI ricalcolati ad ogni batch con un delete + insert esplicito
  scoped alla partizione toccata dal batch (`cfg_kpi.partition_column`: data per i KPI
  temporali, prodotto per il ranking dei top_products), definiti come SQL in `cfg_kpi`
  (config-driven anche il gold).

Tutte le tabelle di configurazione e di controllo vivono in `data/meta.db`. La logica di
ogni livello vive rispettivamente in `src/pipeline/landing_2_bronze.py`,
`src/pipeline/bronze_2_silver.py` e `src/pipeline/silver_2_gold.py`.

## Come avviare la demo

```bash
cd ibsa-sales-reporting-demo
python -m venv .venv && source .venv/Scripts/activate   # o .venv\Scripts\activate su cmd
pip install -r requirements.txt

python scripts/seed_landing_data.py   # genera i CSV di landing (4 cartelle x 5 country)
python webapp/app.py                  # bootstrap DB + avvio Flask + scheduler (tick ogni 30 sec)
```

Apri `http://localhost:5000`: la dashboard parte **vuota** e si popola quando lo
scheduler processa i batch di landing, uno ogni 30 secondi, scandendo le cartelle in
ordine (1, 2, 3, 4) e dentro ciascuna le 5 country (Italia, Svizzera, Cina, Germania,
Inghilterra) — 20 batch in tutto.

Per resettare la demo (ripartire da zero senza rigenerare i CSV di landing):

```bash
python scripts/reset_demo.py
```

## Test

```bash
pytest tests/
```
