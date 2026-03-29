from __future__ import annotations

import csv
import io
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from app.collector import JobCollector
from app.config import Settings
from app.db import Database
from app.digest import DigestService
from app.scheduler import build_scheduler


def _run_to_dict(run) -> dict[str, Any] | None:
    if run is None:
        return None
    return {
        "run_id": run.run_id,
        "started_at": run.started_at.isoformat(),
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "status": run.status,
        "total_fetched": run.total_fetched,
        "total_kept": run.total_kept,
        "total_new": run.total_new,
        "total_updated": run.total_updated,
        "errors": run.errors,
    }


def _job_filters(
    role: str | None,
    source: str | None,
    location: str | None,
    early_career: bool | None,
    min_experience_score: float | None,
    new_since_hours: int | None,
) -> dict[str, Any]:
    return {
        "role": role,
        "source": source,
        "location": location,
        "early_career": early_career,
        "min_experience_score": min_experience_score,
        "new_since_hours": new_since_hours,
    }


def _parse_optional_str(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def _parse_optional_bool(value: str | None) -> bool | None:
    cleaned = _parse_optional_str(value)
    if cleaned is None:
        return None
    lowered = cleaned.lower()
    if lowered in {"true", "1", "yes", "on"}:
        return True
    if lowered in {"false", "0", "no", "off"}:
        return False
    return None


def _parse_optional_float(value: str | None, minimum: float, maximum: float) -> float | None:
    cleaned = _parse_optional_str(value)
    if cleaned is None:
        return None
    try:
        parsed = float(cleaned)
    except ValueError:
        return None
    if parsed < minimum or parsed > maximum:
        return None
    return parsed


def _parse_optional_int(value: str | None, minimum: int, maximum: int) -> int | None:
    cleaned = _parse_optional_str(value)
    if cleaned is None:
        return None
    try:
        parsed = int(cleaned)
    except ValueError:
        return None
    if parsed < minimum or parsed > maximum:
        return None
    return parsed


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    settings.ensure_db_dir()
    templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        db = Database(settings.db_path)
        db.init()
        collector = JobCollector(settings, db)
        digest_service = DigestService(settings, db)
        app.state.settings = settings
        app.state.db = db
        app.state.collector = collector
        app.state.digest_service = digest_service
        app.state.scheduler = None
        if settings.enable_scheduler:
            scheduler = build_scheduler(settings, collector, digest_service)
            scheduler.start()
            app.state.scheduler = scheduler
        yield
        if app.state.scheduler is not None:
            app.state.scheduler.shutdown(wait=False)

    app = FastAPI(title="PM Job Hunter", version="0.1.0", lifespan=lifespan)

    @app.get("/", response_class=HTMLResponse)
    def dashboard(
        request: Request,
        role: str | None = None,
        source: str | None = None,
        location: str | None = None,
        early_career: str | None = None,
        min_experience_score: str | None = None,
        new_since_hours: str | None = "24",
    ):
        parsed_early_career = _parse_optional_bool(early_career)
        parsed_min_score = _parse_optional_float(min_experience_score, minimum=0.0, maximum=1.0)
        parsed_new_since = _parse_optional_int(new_since_hours, minimum=1, maximum=168)
        filters = _job_filters(
            role=_parse_optional_str(role),
            source=_parse_optional_str(source),
            location=_parse_optional_str(location),
            early_career=parsed_early_career,
            min_experience_score=parsed_min_score,
            new_since_hours=parsed_new_since,
        )
        jobs = request.app.state.db.list_jobs(**filters, limit=300)
        latest_run = _run_to_dict(request.app.state.db.get_latest_run())
        return templates.TemplateResponse(
            request,
            "dashboard.html",
            {
                "jobs": jobs,
                "latest_run": latest_run,
                "filters": filters,
                "timezone": settings.app_timezone,
                "role_options": [
                    ("", "All roles"),
                    ("product_owner", "Product Owner"),
                    ("product_manager", "Product Manager"),
                    ("associate_product_manager", "APM"),
                ],
                "source_options": [
                    ("", "All sources"),
                    ("linkedin_public", "LinkedIn (Public)"),
                    ("greenhouse", "Greenhouse"),
                    ("lever", "Lever"),
                ],
                "location_options": [
                    ("", "All locations"),
                    ("Alexandria", "Alexandria"),
                    ("Cairo", "Cairo"),
                    ("__remote_outside__", "Remote (Outside Egypt)"),
                    ("Remote", "Remote"),
                    ("Egypt", "Egypt"),
                    ("United Arab Emirates", "UAE"),
                    ("Saudi Arabia", "Saudi Arabia"),
                    ("Qatar", "Qatar"),
                    ("Kuwait", "Kuwait"),
                    ("Bahrain", "Bahrain"),
                    ("Oman", "Oman"),
                    ("Jordan", "Jordan"),
                    ("Morocco", "Morocco"),
                ],
                "score_options": [
                    ("", "Any score"),
                    ("0.3", ">= 0.30"),
                    ("0.45", ">= 0.45"),
                    ("0.6", ">= 0.60"),
                    ("0.75", ">= 0.75"),
                ],
                "freshness_options": [
                    ("", "Any time"),
                    ("24", "Last 24h"),
                    ("72", "Last 3 days"),
                    ("168", "Last 7 days"),
                ],
            },
        )

    @app.post("/runs/manual")
    def run_manual(request: Request):
        run = request.app.state.collector.run_once()
        return JSONResponse(_run_to_dict(run))

    @app.post("/digest/manual")
    def digest_manual(request: Request):
        result = request.app.state.digest_service.send_daily_digest(hours=24)
        return JSONResponse(result)

    @app.get("/runs/latest")
    def get_latest_run(request: Request):
        run = request.app.state.db.get_latest_run()
        if run is None:
            return JSONResponse({"message": "No runs yet"}, status_code=404)
        return JSONResponse(_run_to_dict(run))

    @app.get("/jobs")
    def get_jobs(
        request: Request,
        role: str | None = None,
        source: str | None = None,
        location: str | None = None,
        early_career: str | None = None,
        min_experience_score: str | None = None,
        new_since_hours: str | None = None,
    ):
        filters = _job_filters(
            role=_parse_optional_str(role),
            source=_parse_optional_str(source),
            location=_parse_optional_str(location),
            early_career=_parse_optional_bool(early_career),
            min_experience_score=_parse_optional_float(min_experience_score, minimum=0.0, maximum=1.0),
            new_since_hours=_parse_optional_int(new_since_hours, minimum=1, maximum=720),
        )
        jobs = request.app.state.db.list_jobs(**filters, limit=500)
        return {"count": len(jobs), "items": jobs}

    @app.get("/jobs/export.csv")
    def export_jobs_csv(
        request: Request,
        role: str | None = None,
        source: str | None = None,
        location: str | None = None,
        early_career: str | None = None,
        min_experience_score: str | None = None,
        new_since_hours: str | None = None,
    ):
        filters = _job_filters(
            role=_parse_optional_str(role),
            source=_parse_optional_str(source),
            location=_parse_optional_str(location),
            early_career=_parse_optional_bool(early_career),
            min_experience_score=_parse_optional_float(min_experience_score, minimum=0.0, maximum=1.0),
            new_since_hours=_parse_optional_int(new_since_hours, minimum=1, maximum=720),
        )
        jobs = request.app.state.db.list_jobs(**filters, limit=5000)

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(
            [
                "title",
                "company",
                "location",
                "source",
                "role_family",
                "early_career_score",
                "apply_url",
                "job_url",
                "posted_at",
                "content_updated_at",
            ]
        )
        for row in jobs:
            writer.writerow(
                [
                    row["title"],
                    row["company"],
                    row["location"],
                    row["source"],
                    row["role_family"],
                    row["early_career_score"],
                    row["apply_url"],
                    row["job_url"],
                    row["posted_at"],
                    row["content_updated_at"],
                ]
            )
        output.seek(0)

        return StreamingResponse(
            output,
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="pm_jobs.csv"'},
        )

    return app


app = create_app()
