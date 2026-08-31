"""Scheduler in-process: esegue la prossima cartella di landing (tutte le country insieme)
ogni 30 secondi reali."""
import logging

from apscheduler.schedulers.background import BackgroundScheduler

from src.pipeline.orchestrator import run_next_landing_dir

logger = logging.getLogger(__name__)

TICK_SECONDS = 30


def _tick():
    result = run_next_landing_dir()
    logger.info("scheduler tick: %s", result)


def start_scheduler() -> BackgroundScheduler:
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        _tick,
        trigger="interval",
        seconds=TICK_SECONDS,
        id="process_next_batch",
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    logger.info("scheduler avviato: un batch ogni %s secondi", TICK_SECONDS)
    return scheduler
