from __future__ import annotations

import argparse
import json

from app.collector import JobCollector
from app.config import Settings
from app.db import Database
from app.digest import DigestService
from app.facebook_collector import FacebookCollector


def _bootstrap() -> tuple[Settings, Database]:
    settings = Settings.from_env()
    settings.ensure_runtime_dirs()
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


def run_facebook_login() -> int:
    settings, db = _bootstrap()
    collector = FacebookCollector(settings, db)
    result = collector.bootstrap_login()
    print(json.dumps(result))
    return 0


def run_facebook_discover() -> int:
    settings, db = _bootstrap()
    collector = FacebookCollector(settings, db)
    result = collector.run_discovery()
    print(
        json.dumps(
            {
                "run_id": result.run_id,
                "mode": result.mode,
                "status": result.status,
                "fetched": result.total_fetched,
                "kept": result.total_kept,
                "new": result.total_new,
                "updated": result.total_updated,
                "errors": result.errors,
            }
        )
    )
    return 0 if result.status not in {"failed", "disabled"} else 1


def run_facebook_collect() -> int:
    settings, db = _bootstrap()
    collector = FacebookCollector(settings, db)
    result = collector.run_once()
    print(
        json.dumps(
            {
                "run_id": result.run_id,
                "mode": result.mode,
                "status": result.status,
                "fetched": result.total_fetched,
                "kept": result.total_kept,
                "new": result.total_new,
                "updated": result.total_updated,
                "errors": result.errors,
            }
        )
    )
    return 0 if result.status not in {"failed", "disabled"} else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="PM Job Hunter CLI")
    parser.add_argument(
        "command",
        choices=["collect", "digest", "facebook-login", "facebook-discover", "facebook-collect"],
    )
    args = parser.parse_args()

    if args.command == "collect":
        return run_collect()
    if args.command == "digest":
        return run_digest()
    if args.command == "facebook-login":
        return run_facebook_login()
    if args.command == "facebook-discover":
        return run_facebook_discover()
    return run_facebook_collect()


if __name__ == "__main__":
    raise SystemExit(main())
