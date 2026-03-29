from __future__ import annotations

import csv
import io
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.collector import JobCollector
from app.config import Settings
from app.db import Database
from app.digest import DigestService
from app.facebook_collector import FacebookCollector
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


def _facebook_run_to_dict(run) -> dict[str, Any] | None:
    if run is None:
        return None
    return {
        "run_id": run.run_id,
        "mode": run.mode,
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


def _facebook_filters(
    group: str | None,
    category: str | None,
    has_phone: bool | None,
    new_since_hours: int | None,
) -> dict[str, Any]:
    return {
        "group": group,
        "category": category,
        "has_phone": has_phone,
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


def _build_facebook_status(
    *,
    settings: Settings,
    db: Database,
) -> dict[str, Any]:
    session_ready = Path(settings.facebook_storage_state_path).exists()
    approved_groups_count = len(db.list_facebook_groups(active_only=True, limit=10_000))
    latest_collect_run = _facebook_run_to_dict(db.get_latest_facebook_run(mode="collect"))
    latest_discovery_run = _facebook_run_to_dict(db.get_latest_facebook_run(mode="discovery"))

    blocking_reason: str | None = None
    if not settings.facebook_enabled:
        blocking_reason = "Facebook scraping is disabled in configuration."
    elif not session_ready:
        blocking_reason = "Facebook login session not found. Run facebook-login first."
    elif approved_groups_count == 0:
        blocking_reason = "No approved active Facebook groups. Approve groups first."

    return {
        "facebook_enabled": settings.facebook_enabled,
        "session_ready": session_ready,
        "approved_groups_count": approved_groups_count,
        "latest_collect_run": latest_collect_run,
        "latest_discovery_run": latest_discovery_run,
        "can_collect": blocking_reason is None,
        "blocking_reason": blocking_reason,
    }


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    settings.ensure_runtime_dirs()
    templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        db = Database(settings.db_path)
        db.init()
        collector = JobCollector(settings, db)
        facebook_collector = FacebookCollector(settings, db)
        digest_service = DigestService(settings, db)
        app.state.settings = settings
        app.state.db = db
        app.state.collector = collector
        app.state.facebook_collector = facebook_collector
        app.state.digest_service = digest_service
        app.state.scheduler = None
        if settings.enable_scheduler:
            scheduler = build_scheduler(settings, collector, digest_service, facebook_collector)
            scheduler.start()
            app.state.scheduler = scheduler
        yield
        if app.state.scheduler is not None:
            app.state.scheduler.shutdown(wait=False)

    app = FastAPI(title="PM Job Hunter", version="0.2.0", lifespan=lifespan)
    app.mount(
        "/assets/screenshots",
        StaticFiles(directory=settings.facebook_screenshots_dir),
        name="facebook_screenshots",
    )

    @app.get("/", response_class=HTMLResponse)
    def dashboard(
        request: Request,
        tab: str | None = "pm",
        role: str | None = None,
        source: str | None = None,
        location: str | None = None,
        early_career: str | None = None,
        min_experience_score: str | None = None,
        new_since_hours: str | None = "24",
        fb_group: str | None = None,
        fb_category: str | None = None,
        fb_has_phone: str | None = None,
        fb_new_since_hours: str | None = "24",
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

        facebook_filters = _facebook_filters(
            group=_parse_optional_str(fb_group),
            category=_parse_optional_str(fb_category),
            has_phone=_parse_optional_bool(fb_has_phone),
            new_since_hours=_parse_optional_int(fb_new_since_hours, minimum=1, maximum=720),
        )

        jobs = request.app.state.db.list_jobs(**filters, limit=300)
        facebook_posts = request.app.state.db.list_facebook_posts(**facebook_filters, limit=300)
        facebook_candidates = request.app.state.db.list_facebook_group_candidates(status="pending", limit=100)
        facebook_groups = request.app.state.db.list_facebook_groups(active_only=False, limit=300)

        latest_run = _run_to_dict(request.app.state.db.get_latest_run())
        latest_facebook_run = _facebook_run_to_dict(request.app.state.db.get_latest_facebook_run(mode="collect"))
        latest_discovery_run = _facebook_run_to_dict(request.app.state.db.get_latest_facebook_run(mode="discovery"))
        active_tab = "facebook" if _parse_optional_str(tab) == "facebook" else "pm"

        return templates.TemplateResponse(
            request,
            "dashboard.html",
            {
                "active_tab": active_tab,
                "jobs": jobs,
                "latest_run": latest_run,
                "filters": filters,
                "facebook_posts": facebook_posts,
                "facebook_candidates": facebook_candidates,
                "facebook_groups": facebook_groups,
                "facebook_filters": facebook_filters,
                "latest_facebook_run": latest_facebook_run,
                "latest_discovery_run": latest_discovery_run,
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
                "facebook_category_options": [
                    ("", "All categories"),
                    ("cold_calling", "Cold Calling"),
                    ("sales", "Sales"),
                    ("customer_support", "Customer Support"),
                    ("data_entry", "Data Entry"),
                    ("other_remote_job", "Other Remote Jobs"),
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

    @app.post("/facebook/login/bootstrap")
    def facebook_login_bootstrap(request: Request):
        result = request.app.state.facebook_collector.bootstrap_login()
        return JSONResponse(result)

    @app.post("/facebook/discovery/run")
    def facebook_discovery_run(request: Request):
        run = request.app.state.facebook_collector.run_discovery()
        return JSONResponse(_facebook_run_to_dict(run))

    @app.get("/facebook/groups/candidates")
    def facebook_group_candidates(request: Request, status: str | None = None):
        items = request.app.state.db.list_facebook_group_candidates(status=_parse_optional_str(status), limit=500)
        return {"count": len(items), "items": items}

    @app.get("/facebook/status")
    def facebook_status(request: Request):
        return _build_facebook_status(
            settings=request.app.state.settings,
            db=request.app.state.db,
        )

    @app.post("/facebook/groups/{group_id}/approve")
    def approve_facebook_group(request: Request, group_id: str):
        approved = request.app.state.db.approve_facebook_group(group_external_id=group_id)
        if approved is None:
            return JSONResponse({"message": "group not found"}, status_code=404)
        return JSONResponse({"status": "approved", "group": approved})

    @app.post("/facebook/groups/{group_id}/disable")
    def disable_facebook_group(request: Request, group_id: str):
        disabled = request.app.state.db.disable_facebook_group(group_external_id=group_id)
        if not disabled:
            return JSONResponse({"message": "group not found"}, status_code=404)
        return JSONResponse({"status": "disabled", "group_external_id": group_id})

    @app.post("/facebook/runs/manual")
    def facebook_collect_manual(request: Request):
        run = request.app.state.facebook_collector.run_once()
        return JSONResponse(_facebook_run_to_dict(run))

    @app.get("/facebook/runs/latest")
    def facebook_latest_run(request: Request, mode: str | None = "collect"):
        parsed_mode = _parse_optional_str(mode) or "collect"
        run = request.app.state.db.get_latest_facebook_run(mode=parsed_mode)
        if run is None:
            return JSONResponse({"message": "No facebook runs yet"}, status_code=404)
        return JSONResponse(_facebook_run_to_dict(run))

    @app.get("/facebook/posts")
    def get_facebook_posts(
        request: Request,
        group: str | None = None,
        category: str | None = None,
        has_phone: str | None = None,
        new_since_hours: str | None = None,
    ):
        filters = _facebook_filters(
            group=_parse_optional_str(group),
            category=_parse_optional_str(category),
            has_phone=_parse_optional_bool(has_phone),
            new_since_hours=_parse_optional_int(new_since_hours, minimum=1, maximum=720),
        )
        items = request.app.state.db.list_facebook_posts(**filters, limit=500)
        return {"count": len(items), "items": items}

    @app.get("/facebook/posts/export.csv")
    def export_facebook_posts_csv(
        request: Request,
        group: str | None = None,
        category: str | None = None,
        has_phone: str | None = None,
        new_since_hours: str | None = None,
    ):
        filters = _facebook_filters(
            group=_parse_optional_str(group),
            category=_parse_optional_str(category),
            has_phone=_parse_optional_bool(has_phone),
            new_since_hours=_parse_optional_int(new_since_hours, minimum=1, maximum=720),
        )
        items = request.app.state.db.list_facebook_posts(**filters, limit=5000)

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(
            [
                "group_external_id",
                "group_name",
                "post_url",
                "category_tag",
                "phone_numbers",
                "whatsapp_links",
                "screenshot_path",
                "posted_at",
                "content_updated_at",
                "post_excerpt",
            ]
        )
        for row in items:
            writer.writerow(
                [
                    row["group_external_id"],
                    row["group_name"],
                    row["post_url"],
                    row["category_tag"],
                    ";".join(row.get("phone_numbers", [])),
                    ";".join(row.get("whatsapp_links", [])),
                    row.get("screenshot_path") or "",
                    row.get("posted_at"),
                    row.get("content_updated_at"),
                    row.get("post_excerpt"),
                ]
            )

        output.seek(0)
        return StreamingResponse(
            output,
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="facebook_remote_jobs.csv"'},
        )

    return app


app = create_app()
