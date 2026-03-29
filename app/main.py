from __future__ import annotations

import csv
import io
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
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
        early_career: bool | None = None,
        min_experience_score: float | None = Query(default=None, ge=0.0, le=1.0),
        new_since_hours: int | None = Query(default=24, ge=1, le=168),
    ):
        filters = _job_filters(
            role=role,
            source=source,
            location=location,
            early_career=early_career,
            min_experience_score=min_experience_score,
            new_since_hours=new_since_hours,
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
        early_career: bool | None = None,
        min_experience_score: float | None = Query(default=None, ge=0.0, le=1.0),
        new_since_hours: int | None = Query(default=None, ge=1, le=720),
    ):
        filters = _job_filters(
            role=role,
            source=source,
            location=location,
            early_career=early_career,
            min_experience_score=min_experience_score,
            new_since_hours=new_since_hours,
        )
        jobs = request.app.state.db.list_jobs(**filters, limit=500)
        return {"count": len(jobs), "items": jobs}

    @app.get("/jobs/export.csv")
    def export_jobs_csv(
        request: Request,
        role: str | None = None,
        source: str | None = None,
        location: str | None = None,
        early_career: bool | None = None,
        min_experience_score: float | None = Query(default=None, ge=0.0, le=1.0),
        new_since_hours: int | None = Query(default=None, ge=1, le=720),
    ):
        filters = _job_filters(
            role=role,
            source=source,
            location=location,
            early_career=early_career,
            min_experience_score=min_experience_score,
            new_since_hours=new_since_hours,
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

