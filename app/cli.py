from __future__ import annotations

import argparse
import json

from app.collector import JobCollector
from app.config import Settings
from app.db import Database
from app.digest import DigestService


def _bootstrap() -> tuple[Settings, Database]:
    settings = Settings.from_env()
    settings.ensure_db_dir()
    db = Database(settings.db_path)
    db.init()
    return settings, db


def run_collect() -> int:
    settings, db = _bootstrap()
    collector = JobCollector(settings, db)
    result = collector.run_once()
    print(
        json.dumps(
            {
                "run_id": result.run_id,
                "status": result.status,
                "fetched": result.total_fetched,
                "kept": result.total_kept,
                "new": result.total_new,
                "updated": result.total_updated,
                "errors": result.errors,
            }
        )
    )
    return 0 if result.status != "failed" else 1


def run_digest() -> int:
    settings, db = _bootstrap()
    digest = DigestService(settings, db)
    result = digest.send_daily_digest(hours=24)
    print(json.dumps(result))
    return 0 if result.get("status") != "missing_config" else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="PM Job Hunter CLI")
    parser.add_argument("command", choices=["collect", "digest"])
    args = parser.parse_args()

    if args.command == "collect":
        return run_collect()
    return run_digest()


if __name__ == "__main__":
    raise SystemExit(main())

