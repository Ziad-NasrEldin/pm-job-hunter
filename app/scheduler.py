from __future__ import annotations

import logging
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.collector import JobCollector
from app.config import Settings
from app.digest import DigestService

logger = logging.getLogger(__name__)


def build_scheduler(
    settings: Settings, collector: JobCollector, digest_service: DigestService
) -> BackgroundScheduler:
    scheduler = BackgroundScheduler(timezone=ZoneInfo(settings.app_timezone))

    def run_collection_job() -> None:
        try:
            result = collector.run_once()
            logger.info("Collection job completed: run_id=%s status=%s", result.run_id, result.status)
        except Exception:  # noqa: BLE001
            logger.exception("Collection job failed")

    def run_digest_job() -> None:
        try:
            result = digest_service.send_daily_digest(hours=24)
            logger.info("Digest job completed: %s", result.get("status"))
        except Exception:  # noqa: BLE001
            logger.exception("Digest job failed")

    scheduler.add_job(
        run_collection_job,
        trigger=CronTrigger(hour=9, minute=0),
        id="daily_collection",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        run_digest_job,
        trigger=CronTrigger(hour=9, minute=15),
        id="daily_digest",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    return scheduler

