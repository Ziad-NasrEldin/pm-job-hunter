from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from hashlib import sha1
from threading import Lock
from typing import Any

from app.models import (
    DigestItem,
    FacebookGroupCandidate,
    FacebookRunEvent,
    FacebookPost,
    FacebookRunSummary,
    RunSummary,
    ScoredJob,
)


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC).isoformat()
    return dt.astimezone(UTC).isoformat()


def _parse(dt: str | None) -> datetime | None:
    if not dt:
        return None
    return datetime.fromisoformat(dt)


LOCATION_PRIORITY_SQL = """
CASE
    WHEN lower(location) LIKE '%alexandria%' THEN 0
    WHEN lower(location) LIKE '%cairo%' THEN 1
    WHEN lower(location) LIKE '%remote%'
         AND lower(location) NOT LIKE '%egypt%'
         AND lower(location) NOT LIKE '%alexandria%'
         AND lower(location) NOT LIKE '%cairo%' THEN 2
    ELSE 3
END
"""


class Database:
    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = Lock()

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def init(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    normalized_title TEXT NOT NULL,
                    company TEXT NOT NULL,
                    location TEXT NOT NULL,
                    description TEXT NOT NULL,
                    job_url TEXT NOT NULL,
                    apply_url TEXT NOT NULL,
                    posted_at TEXT,
                    role_family TEXT NOT NULL,
                    role_priority INTEGER NOT NULL,
                    dedupe_key TEXT NOT NULL UNIQUE,
                    early_career_score REAL NOT NULL,
                    is_early_career INTEGER NOT NULL,
                    seniority_blocked INTEGER NOT NULL,
                    years_min INTEGER,
                    years_max INTEGER,
                    metadata_json TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    content_updated_at TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1
                );
                CREATE TABLE IF NOT EXISTS runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    status TEXT NOT NULL,
                    total_fetched INTEGER NOT NULL DEFAULT 0,
                    total_kept INTEGER NOT NULL DEFAULT 0,
                    total_new INTEGER NOT NULL DEFAULT 0,
                    total_updated INTEGER NOT NULL DEFAULT 0,
                    errors_json TEXT NOT NULL DEFAULT '[]'
                );
                CREATE TABLE IF NOT EXISTS facebook_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mode TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    status TEXT NOT NULL,
                    total_fetched INTEGER NOT NULL DEFAULT 0,
                    total_kept INTEGER NOT NULL DEFAULT 0,
                    total_new INTEGER NOT NULL DEFAULT 0,
                    total_updated INTEGER NOT NULL DEFAULT 0,
                    errors_json TEXT NOT NULL DEFAULT '[]'
                );
                CREATE TABLE IF NOT EXISTS facebook_run_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    stage TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    message TEXT NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS facebook_run_checkpoints (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    mode TEXT NOT NULL,
                    last_success_group_id TEXT,
                    next_group_index INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS facebook_group_candidates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_external_id TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    group_url TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    relevance_score REAL NOT NULL DEFAULT 0.0,
                    discovered_keyword TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'pending',
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS facebook_groups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_external_id TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    group_url TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    approved_at TEXT NOT NULL,
                    last_crawled_at TEXT,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS facebook_posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_external_id TEXT NOT NULL,
                    group_name TEXT NOT NULL,
                    post_external_id TEXT NOT NULL,
                    post_url TEXT NOT NULL,
                    post_text TEXT NOT NULL,
                    post_excerpt TEXT NOT NULL,
                    posted_at TEXT,
                    category_tag TEXT NOT NULL,
                    is_remote INTEGER NOT NULL DEFAULT 1,
                    phone_numbers_json TEXT NOT NULL DEFAULT '[]',
                    whatsapp_links_json TEXT NOT NULL DEFAULT '[]',
                    screenshot_path TEXT,
                    raw_snapshot_path TEXT,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    dedupe_key TEXT NOT NULL UNIQUE,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    content_updated_at TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    lead_status TEXT NOT NULL DEFAULT 'active',
                    lead_note TEXT NOT NULL DEFAULT '',
                    reviewed_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_jobs_role ON jobs(role_priority DESC, early_career_score DESC);
                CREATE INDEX IF NOT EXISTS idx_jobs_updated ON jobs(content_updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_facebook_candidates_status ON facebook_group_candidates(status, relevance_score DESC);
                CREATE INDEX IF NOT EXISTS idx_facebook_groups_active ON facebook_groups(is_active, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_facebook_posts_updated ON facebook_posts(content_updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_facebook_posts_group ON facebook_posts(group_external_id, content_updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_facebook_posts_lead_status ON facebook_posts(lead_status, content_updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_facebook_run_events_run_id ON facebook_run_events(run_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_facebook_run_checkpoints_mode ON facebook_run_checkpoints(mode, updated_at DESC);
                """
            )
            self._ensure_facebook_schema(conn)
            conn.commit()

    def _ensure_facebook_schema(self, conn: sqlite3.Connection) -> None:
        facebook_posts_cols = {row["name"] for row in conn.execute("PRAGMA table_info(facebook_posts)").fetchall()}
        if "lead_status" not in facebook_posts_cols:
            conn.execute("ALTER TABLE facebook_posts ADD COLUMN lead_status TEXT NOT NULL DEFAULT 'active'")
        if "lead_note" not in facebook_posts_cols:
            conn.execute("ALTER TABLE facebook_posts ADD COLUMN lead_note TEXT NOT NULL DEFAULT ''")
        if "reviewed_at" not in facebook_posts_cols:
            conn.execute("ALTER TABLE facebook_posts ADD COLUMN reviewed_at TEXT")

    def create_run(self, started_at: datetime) -> int:
        with self._lock, self.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO runs(started_at, status)
                VALUES(?, ?)
                """,
                (_iso(started_at), "running"),
            )
            conn.commit()
            return int(cur.lastrowid)

    def finalize_run(
        self,
        run_id: int,
        status: str,
        total_fetched: int,
        total_kept: int,
        total_new: int,
        total_updated: int,
        errors: list[str],
        finished_at: datetime,
    ) -> None:
        with self._lock, self.connect() as conn:
            conn.execute(
                """
                UPDATE runs
                SET finished_at = ?, status = ?, total_fetched = ?, total_kept = ?,
                    total_new = ?, total_updated = ?, errors_json = ?
                WHERE id = ?
                """,
                (
                    _iso(finished_at),
                    status,
                    total_fetched,
                    total_kept,
                    total_new,
                    total_updated,
                    json.dumps(errors),
                    run_id,
                ),
            )
            conn.commit()

    def create_facebook_run(self, started_at: datetime, mode: str) -> int:
        with self._lock, self.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO facebook_runs(mode, started_at, status)
                VALUES(?, ?, ?)
                """,
                (mode, _iso(started_at), "running"),
            )
            conn.commit()
            return int(cur.lastrowid)

    def finalize_facebook_run(
        self,
        run_id: int,
        status: str,
        total_fetched: int,
        total_kept: int,
        total_new: int,
        total_updated: int,
        errors: list[str],
        finished_at: datetime,
    ) -> None:
        with self._lock, self.connect() as conn:
            conn.execute(
                """
                UPDATE facebook_runs
                SET finished_at = ?, status = ?, total_fetched = ?, total_kept = ?,
                    total_new = ?, total_updated = ?, errors_json = ?
                WHERE id = ?
                """,
                (
                    _iso(finished_at),
                    status,
                    total_fetched,
                    total_kept,
                    total_new,
                    total_updated,
                    json.dumps(errors),
                    run_id,
                ),
            )
            conn.commit()

    def _content_hash(self, job: ScoredJob) -> str:
        blob = "|".join(
            [
                job.title,
                job.company,
                job.location,
                job.description,
                job.apply_url,
                job.job_url,
                str(job.posted_at),
                f"{job.early_career_score:.3f}",
                str(job.role_priority),
                str(job.years_min),
                str(job.years_max),
            ]
        )
        return sha1(blob.encode("utf-8")).hexdigest()

    def upsert_job(self, job: ScoredJob, now: datetime | None = None) -> str:
        now = now or _utcnow()
        now_iso = _iso(now)
        metadata_json = json.dumps(job.metadata or {})
        posted_iso = _iso(job.posted_at)
        content_hash = self._content_hash(job)

        with self._lock, self.connect() as conn:
            existing = conn.execute(
                "SELECT id, content_hash FROM jobs WHERE dedupe_key = ?",
                (job.dedupe_key,),
            ).fetchone()

            if existing is None:
                conn.execute(
                    """
                    INSERT INTO jobs(
                        source, external_id, title, normalized_title, company, location,
                        description, job_url, apply_url, posted_at, role_family, role_priority,
                        dedupe_key, early_career_score, is_early_career, seniority_blocked, years_min,
                        years_max, metadata_json, first_seen_at, last_seen_at, content_updated_at,
                        content_hash, is_active
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                    """,
                    (
                        job.source,
                        job.external_id,
                        job.title,
                        job.normalized_title,
                        job.company,
                        job.location,
                        job.description,
                        job.job_url,
                        job.apply_url,
                        posted_iso,
                        job.role_family,
                        job.role_priority,
                        job.dedupe_key,
                        job.early_career_score,
                        int(job.is_early_career),
                        int(job.seniority_blocked),
                        job.years_min,
                        job.years_max,
                        metadata_json,
                        now_iso,
                        now_iso,
                        now_iso,
                        content_hash,
                    ),
                )
                conn.commit()
                return "new"

            if existing["content_hash"] != content_hash:
                conn.execute(
                    """
                    UPDATE jobs
                    SET source = ?, external_id = ?, title = ?, normalized_title = ?,
                        company = ?, location = ?, description = ?, job_url = ?, apply_url = ?,
                        posted_at = ?, role_family = ?, role_priority = ?, early_career_score = ?,
                        is_early_career = ?, seniority_blocked = ?, years_min = ?, years_max = ?,
                        metadata_json = ?, last_seen_at = ?, content_updated_at = ?, content_hash = ?,
                        is_active = 1
                    WHERE dedupe_key = ?
                    """,
                    (
                        job.source,
                        job.external_id,
                        job.title,
                        job.normalized_title,
                        job.company,
                        job.location,
                        job.description,
                        job.job_url,
                        job.apply_url,
                        posted_iso,
                        job.role_family,
                        job.role_priority,
                        job.early_career_score,
                        int(job.is_early_career),
                        int(job.seniority_blocked),
                        job.years_min,
                        job.years_max,
                        metadata_json,
                        now_iso,
                        now_iso,
                        content_hash,
                        job.dedupe_key,
                    ),
                )
                conn.commit()
                return "updated"

            conn.execute(
                "UPDATE jobs SET last_seen_at = ?, is_active = 1 WHERE dedupe_key = ?",
                (now_iso, job.dedupe_key),
            )
            conn.commit()
            return "unchanged"

    def prune_old_jobs(self, retention_days: int) -> int:
        cutoff = _iso(_utcnow() - timedelta(days=retention_days))
        with self._lock, self.connect() as conn:
            cur = conn.execute("DELETE FROM jobs WHERE last_seen_at < ?", (cutoff,))
            conn.commit()
            return cur.rowcount

    def get_latest_run(self) -> RunSummary | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM runs ORDER BY id DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        return RunSummary(
            run_id=row["id"],
            started_at=_parse(row["started_at"]) or _utcnow(),
            finished_at=_parse(row["finished_at"]),
            status=row["status"],
            total_fetched=row["total_fetched"],
            total_kept=row["total_kept"],
            total_new=row["total_new"],
            total_updated=row["total_updated"],
            errors=json.loads(row["errors_json"] or "[]"),
        )

    def get_latest_facebook_run(self, mode: str | None = None) -> FacebookRunSummary | None:
        query = "SELECT * FROM facebook_runs"
        values: list[Any] = []
        if mode:
            query += " WHERE mode = ?"
            values.append(mode)
        query += " ORDER BY id DESC LIMIT 1"

        with self.connect() as conn:
            row = conn.execute(query, values).fetchone()
        if row is None:
            return None
        return FacebookRunSummary(
            run_id=row["id"],
            mode=row["mode"],
            started_at=_parse(row["started_at"]) or _utcnow(),
            finished_at=_parse(row["finished_at"]),
            status=row["status"],
            total_fetched=row["total_fetched"],
            total_kept=row["total_kept"],
            total_new=row["total_new"],
            total_updated=row["total_updated"],
            errors=json.loads(row["errors_json"] or "[]"),
        )

    def list_facebook_runs(self, mode: str | None = None, limit: int = 50) -> list[FacebookRunSummary]:
        query = "SELECT * FROM facebook_runs"
        params: list[Any] = []
        if mode:
            query += " WHERE mode = ?"
            params.append(mode)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        items: list[FacebookRunSummary] = []
        for row in rows:
            items.append(
                FacebookRunSummary(
                    run_id=row["id"],
                    mode=row["mode"],
                    started_at=_parse(row["started_at"]) or _utcnow(),
                    finished_at=_parse(row["finished_at"]),
                    status=row["status"],
                    total_fetched=row["total_fetched"],
                    total_kept=row["total_kept"],
                    total_new=row["total_new"],
                    total_updated=row["total_updated"],
                    errors=json.loads(row["errors_json"] or "[]"),
                )
            )
        return items

    def get_facebook_run(self, run_id: int) -> FacebookRunSummary | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM facebook_runs WHERE id = ?", (run_id,)).fetchone()
        if row is None:
            return None
        return FacebookRunSummary(
            run_id=row["id"],
            mode=row["mode"],
            started_at=_parse(row["started_at"]) or _utcnow(),
            finished_at=_parse(row["finished_at"]),
            status=row["status"],
            total_fetched=row["total_fetched"],
            total_kept=row["total_kept"],
            total_new=row["total_new"],
            total_updated=row["total_updated"],
            errors=json.loads(row["errors_json"] or "[]"),
        )

    def add_facebook_run_event(
        self,
        *,
        run_id: int,
        stage: str,
        scope: str,
        message: str,
        payload: dict[str, Any] | None = None,
        created_at: datetime | None = None,
    ) -> None:
        created_at = created_at or _utcnow()
        with self._lock, self.connect() as conn:
            conn.execute(
                """
                INSERT INTO facebook_run_events(run_id, stage, scope, message, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    stage,
                    scope,
                    message,
                    json.dumps(payload or {}),
                    _iso(created_at),
                ),
            )
            conn.commit()

    def list_facebook_run_events(self, run_id: int, limit: int = 500) -> list[FacebookRunEvent]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM facebook_run_events
                WHERE run_id = ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (run_id, limit),
            ).fetchall()
        events: list[FacebookRunEvent] = []
        for row in rows:
            events.append(
                FacebookRunEvent(
                    event_id=row["id"],
                    run_id=row["run_id"],
                    stage=row["stage"],
                    scope=row["scope"],
                    message=row["message"],
                    payload=json.loads(row["payload_json"] or "{}"),
                    created_at=_parse(row["created_at"]) or _utcnow(),
                )
            )
        return events

    def save_facebook_run_checkpoint(
        self,
        *,
        run_id: int,
        mode: str,
        last_success_group_id: str | None,
        next_group_index: int,
        updated_at: datetime | None = None,
    ) -> None:
        updated_at = updated_at or _utcnow()
        with self._lock, self.connect() as conn:
            conn.execute(
                """
                INSERT INTO facebook_run_checkpoints(run_id, mode, last_success_group_id, next_group_index, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (run_id, mode, last_success_group_id, max(0, next_group_index), _iso(updated_at)),
            )
            conn.commit()

    def get_latest_resumable_checkpoint(self, mode: str = "collect") -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT c.run_id, c.mode, c.last_success_group_id, c.next_group_index, c.updated_at
                FROM facebook_run_checkpoints c
                JOIN facebook_runs r ON r.id = c.run_id
                WHERE c.mode = ? AND r.status IN ('failed', 'partial_failed')
                ORDER BY c.id DESC
                LIMIT 1
                """,
                (mode,),
            ).fetchone()
        return dict(row) if row else None

    def upsert_facebook_group_candidate(
        self, candidate: FacebookGroupCandidate, now: datetime | None = None
    ) -> str:
        now = now or _utcnow()
        now_iso = _iso(now)
        metadata_json = json.dumps(candidate.metadata or {})

        with self._lock, self.connect() as conn:
            existing = conn.execute(
                "SELECT id, status, relevance_score, name, group_url, description, discovered_keyword FROM facebook_group_candidates WHERE group_external_id = ?",
                (candidate.group_external_id,),
            ).fetchone()

            if existing is None:
                conn.execute(
                    """
                    INSERT INTO facebook_group_candidates(
                        group_external_id, name, group_url, description, relevance_score,
                        discovered_keyword, status, metadata_json, created_at, updated_at, last_seen_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?)
                    """,
                    (
                        candidate.group_external_id,
                        candidate.name,
                        candidate.group_url,
                        candidate.description,
                        candidate.relevance_score,
                        candidate.discovered_keyword,
                        metadata_json,
                        now_iso,
                        now_iso,
                        now_iso,
                    ),
                )
                conn.commit()
                return "new"

            changed = (
                existing["name"] != candidate.name
                or existing["group_url"] != candidate.group_url
                or existing["description"] != candidate.description
                or existing["discovered_keyword"] != candidate.discovered_keyword
                or float(existing["relevance_score"]) != float(candidate.relevance_score)
            )

            conn.execute(
                """
                UPDATE facebook_group_candidates
                SET name = ?, group_url = ?, description = ?, relevance_score = ?, discovered_keyword = ?,
                    metadata_json = ?, updated_at = ?, last_seen_at = ?
                WHERE group_external_id = ?
                """,
                (
                    candidate.name,
                    candidate.group_url,
                    candidate.description,
                    candidate.relevance_score,
                    candidate.discovered_keyword,
                    metadata_json,
                    now_iso,
                    now_iso,
                    candidate.group_external_id,
                ),
            )
            conn.commit()
            return "updated" if changed else "unchanged"

    def list_facebook_group_candidates(
        self, status: str | None = None, limit: int = 200
    ) -> list[dict[str, Any]]:
        where: list[str] = []
        values: list[Any] = []
        if status:
            where.append("status = ?")
            values.append(status)
        clause = f"WHERE {' AND '.join(where)}" if where else ""
        values.append(limit)
        sql = f"""
        SELECT *
        FROM facebook_group_candidates
        {clause}
        ORDER BY relevance_score DESC, updated_at DESC
        LIMIT ?
        """
        with self.connect() as conn:
            rows = conn.execute(sql, values).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            obj = dict(row)
            obj["metadata"] = json.loads(obj.pop("metadata_json", "{}") or "{}")
            items.append(obj)
        return items

    def approve_facebook_group(self, group_external_id: str, now: datetime | None = None) -> dict[str, Any] | None:
        now = now or _utcnow()
        now_iso = _iso(now)

        with self._lock, self.connect() as conn:
            candidate = conn.execute(
                "SELECT * FROM facebook_group_candidates WHERE group_external_id = ?",
                (group_external_id,),
            ).fetchone()
            if candidate is None:
                return None

            conn.execute(
                """
                UPDATE facebook_group_candidates
                SET status = 'approved', updated_at = ?, last_seen_at = ?
                WHERE group_external_id = ?
                """,
                (now_iso, now_iso, group_external_id),
            )

            existing_group = conn.execute(
                "SELECT id FROM facebook_groups WHERE group_external_id = ?",
                (group_external_id,),
            ).fetchone()
            metadata_json = candidate["metadata_json"] or "{}"
            if existing_group is None:
                conn.execute(
                    """
                    INSERT INTO facebook_groups(
                        group_external_id, name, group_url, is_active, approved_at,
                        metadata_json, created_at, updated_at
                    )
                    VALUES (?, ?, ?, 1, ?, ?, ?, ?)
                    """,
                    (
                        candidate["group_external_id"],
                        candidate["name"],
                        candidate["group_url"],
                        now_iso,
                        metadata_json,
                        now_iso,
                        now_iso,
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE facebook_groups
                    SET name = ?, group_url = ?, is_active = 1, metadata_json = ?, updated_at = ?
                    WHERE group_external_id = ?
                    """,
                    (
                        candidate["name"],
                        candidate["group_url"],
                        metadata_json,
                        now_iso,
                        group_external_id,
                    ),
                )

            conn.commit()
            approved = conn.execute(
                "SELECT * FROM facebook_groups WHERE group_external_id = ?",
                (group_external_id,),
            ).fetchone()

        if approved is None:
            return None
        result = dict(approved)
        result["metadata"] = json.loads(result.pop("metadata_json", "{}") or "{}")
        return result

    def import_facebook_group(
        self,
        *,
        group_external_id: str,
        name: str,
        group_url: str,
        description: str = "",
        metadata: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> str:
        now = now or _utcnow()
        now_iso = _iso(now)
        metadata_json = json.dumps(metadata or {"source": "manual_import"})

        with self._lock, self.connect() as conn:
            existing_candidate = conn.execute(
                "SELECT id, status, name, group_url, description, metadata_json FROM facebook_group_candidates WHERE group_external_id = ?",
                (group_external_id,),
            ).fetchone()
            if existing_candidate is None:
                conn.execute(
                    """
                    INSERT INTO facebook_group_candidates(
                        group_external_id, name, group_url, description, relevance_score,
                        discovered_keyword, status, metadata_json, created_at, updated_at, last_seen_at
                    )
                    VALUES (?, ?, ?, ?, 1.0, 'manual_import', 'approved', ?, ?, ?, ?)
                    """,
                    (
                        group_external_id,
                        name,
                        group_url,
                        description,
                        metadata_json,
                        now_iso,
                        now_iso,
                        now_iso,
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE facebook_group_candidates
                    SET name = ?, group_url = ?, description = ?, status = 'approved',
                        metadata_json = ?, updated_at = ?, last_seen_at = ?
                    WHERE group_external_id = ?
                    """,
                    (
                        name,
                        group_url,
                        description,
                        metadata_json,
                        now_iso,
                        now_iso,
                        group_external_id,
                    ),
                )

            existing_group = conn.execute(
                "SELECT id, name, group_url, is_active, metadata_json FROM facebook_groups WHERE group_external_id = ?",
                (group_external_id,),
            ).fetchone()

            outcome = "new"
            if existing_group is None:
                conn.execute(
                    """
                    INSERT INTO facebook_groups(
                        group_external_id, name, group_url, is_active, approved_at,
                        metadata_json, created_at, updated_at
                    )
                    VALUES (?, ?, ?, 1, ?, ?, ?, ?)
                    """,
                    (
                        group_external_id,
                        name,
                        group_url,
                        now_iso,
                        metadata_json,
                        now_iso,
                        now_iso,
                    ),
                )
                outcome = "new"
            else:
                changed = (
                    existing_group["name"] != name
                    or existing_group["group_url"] != group_url
                    or int(existing_group["is_active"]) != 1
                    or (existing_group["metadata_json"] or "{}") != metadata_json
                )
                conn.execute(
                    """
                    UPDATE facebook_groups
                    SET name = ?, group_url = ?, is_active = 1, metadata_json = ?, updated_at = ?
                    WHERE group_external_id = ?
                    """,
                    (
                        name,
                        group_url,
                        metadata_json,
                        now_iso,
                        group_external_id,
                    ),
                )
                outcome = "updated" if changed else "unchanged"

            conn.commit()
            return outcome

    def disable_facebook_group(self, group_external_id: str, now: datetime | None = None) -> bool:
        now = now or _utcnow()
        now_iso = _iso(now)

        with self._lock, self.connect() as conn:
            updated_group = conn.execute(
                """
                UPDATE facebook_groups
                SET is_active = 0, updated_at = ?
                WHERE group_external_id = ?
                """,
                (now_iso, group_external_id),
            ).rowcount
            conn.execute(
                """
                UPDATE facebook_group_candidates
                SET status = 'disabled', updated_at = ?
                WHERE group_external_id = ?
                """,
                (now_iso, group_external_id),
            )
            conn.commit()
            return updated_group > 0

    def list_facebook_groups(self, active_only: bool = True, limit: int = 500) -> list[dict[str, Any]]:
        where = "WHERE is_active = 1" if active_only else ""
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM facebook_groups
                {where}
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        groups: list[dict[str, Any]] = []
        for row in rows:
            obj = dict(row)
            obj["metadata"] = json.loads(obj.pop("metadata_json", "{}") or "{}")
            groups.append(obj)
        return groups

    def is_facebook_group_tracked(self, group_external_id: str) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM facebook_groups WHERE group_external_id = ? LIMIT 1",
                (group_external_id,),
            ).fetchone()
        return row is not None

    def touch_facebook_group_crawled(self, group_external_id: str, when: datetime | None = None) -> None:
        when = when or _utcnow()
        when_iso = _iso(when)
        with self._lock, self.connect() as conn:
            conn.execute(
                """
                UPDATE facebook_groups
                SET last_crawled_at = ?, updated_at = ?
                WHERE group_external_id = ?
                """,
                (when_iso, when_iso, group_external_id),
            )
            conn.commit()

    def _facebook_post_content_hash(self, post: FacebookPost) -> str:
        blob = "|".join(
            [
                post.group_external_id,
                post.post_external_id,
                post.post_url,
                post.post_text,
                post.post_excerpt,
                post.category_tag,
                str(post.is_remote),
                ",".join(post.phone_numbers),
                ",".join(post.whatsapp_links),
                post.screenshot_path or "",
                post.raw_snapshot_path or "",
                str(post.posted_at),
            ]
        )
        return sha1(blob.encode("utf-8")).hexdigest()

    def upsert_facebook_post(self, post: FacebookPost, now: datetime | None = None) -> str:
        now = now or _utcnow()
        now_iso = _iso(now)
        posted_iso = _iso(post.posted_at)
        metadata_json = json.dumps(post.metadata or {})
        phones_json = json.dumps(post.phone_numbers or [])
        whatsapp_json = json.dumps(post.whatsapp_links or [])
        content_hash = self._facebook_post_content_hash(post)

        with self._lock, self.connect() as conn:
            existing = conn.execute(
                "SELECT id, content_hash FROM facebook_posts WHERE dedupe_key = ?",
                (post.dedupe_key,),
            ).fetchone()

            if existing is None:
                conn.execute(
                    """
                    INSERT INTO facebook_posts(
                        group_external_id, group_name, post_external_id, post_url,
                        post_text, post_excerpt, posted_at, category_tag, is_remote,
                        phone_numbers_json, whatsapp_links_json, screenshot_path, raw_snapshot_path,
                        metadata_json, dedupe_key, first_seen_at, last_seen_at, content_updated_at,
                        content_hash, is_active, lead_status, lead_note, reviewed_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'active', '', NULL)
                    """,
                    (
                        post.group_external_id,
                        post.group_name,
                        post.post_external_id,
                        post.post_url,
                        post.post_text,
                        post.post_excerpt,
                        posted_iso,
                        post.category_tag,
                        int(post.is_remote),
                        phones_json,
                        whatsapp_json,
                        post.screenshot_path,
                        post.raw_snapshot_path,
                        metadata_json,
                        post.dedupe_key,
                        now_iso,
                        now_iso,
                        now_iso,
                        content_hash,
                    ),
                )
                conn.commit()
                return "new"

            if existing["content_hash"] != content_hash:
                conn.execute(
                    """
                    UPDATE facebook_posts
                    SET group_name = ?, post_url = ?, post_text = ?, post_excerpt = ?,
                        posted_at = ?, category_tag = ?, is_remote = ?, phone_numbers_json = ?,
                        whatsapp_links_json = ?, screenshot_path = ?, raw_snapshot_path = ?,
                        metadata_json = ?, last_seen_at = ?, content_updated_at = ?, content_hash = ?,
                        is_active = 1
                    WHERE dedupe_key = ?
                    """,
                    (
                        post.group_name,
                        post.post_url,
                        post.post_text,
                        post.post_excerpt,
                        posted_iso,
                        post.category_tag,
                        int(post.is_remote),
                        phones_json,
                        whatsapp_json,
                        post.screenshot_path,
                        post.raw_snapshot_path,
                        metadata_json,
                        now_iso,
                        now_iso,
                        content_hash,
                        post.dedupe_key,
                    ),
                )
                conn.commit()
                return "updated"

            conn.execute(
                "UPDATE facebook_posts SET last_seen_at = ?, is_active = 1 WHERE dedupe_key = ?",
                (now_iso, post.dedupe_key),
            )
            conn.commit()
            return "unchanged"

    def list_facebook_posts(
        self,
        group: str | None = None,
        category: str | None = None,
        has_phone: bool | None = None,
        lead_status: str | None = "active",
        new_since_hours: int | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        where: list[str] = []
        values: list[Any] = []
        if group:
            where.append("group_external_id = ?")
            values.append(group)
        if category:
            where.append("category_tag = ?")
            values.append(category)
        if has_phone is not None:
            if has_phone:
                where.append("phone_numbers_json != '[]'")
            else:
                where.append("phone_numbers_json = '[]'")
        if lead_status:
            where.append("lead_status = ?")
            values.append(lead_status)
        if new_since_hours is not None:
            cutoff = _iso(_utcnow() - timedelta(hours=new_since_hours))
            where.append("content_updated_at >= ?")
            values.append(cutoff)
        clause = f"WHERE {' AND '.join(where)}" if where else ""
        values.append(limit)

        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM facebook_posts
                {clause}
                ORDER BY content_updated_at DESC
                LIMIT ?
                """,
                values,
            ).fetchall()

        items: list[dict[str, Any]] = []
        for row in rows:
            obj = dict(row)
            obj["phone_numbers"] = json.loads(obj.pop("phone_numbers_json", "[]") or "[]")
            obj["whatsapp_links"] = json.loads(obj.pop("whatsapp_links_json", "[]") or "[]")
            obj["metadata"] = json.loads(obj.pop("metadata_json", "{}") or "{}")
            items.append(obj)
        return items

    def update_facebook_post_status(
        self,
        *,
        dedupe_key: str,
        lead_status: str,
        reviewed_at: datetime | None = None,
    ) -> bool:
        reviewed_at = reviewed_at or _utcnow()
        with self._lock, self.connect() as conn:
            rowcount = conn.execute(
                """
                UPDATE facebook_posts
                SET lead_status = ?, reviewed_at = ?
                WHERE dedupe_key = ?
                """,
                (lead_status, _iso(reviewed_at), dedupe_key),
            ).rowcount
            conn.commit()
            return rowcount > 0

    def update_facebook_post_note(
        self,
        *,
        dedupe_key: str,
        lead_note: str,
        reviewed_at: datetime | None = None,
    ) -> bool:
        reviewed_at = reviewed_at or _utcnow()
        with self._lock, self.connect() as conn:
            rowcount = conn.execute(
                """
                UPDATE facebook_posts
                SET lead_note = ?, reviewed_at = ?
                WHERE dedupe_key = ?
                """,
                ((lead_note or "").strip(), _iso(reviewed_at), dedupe_key),
            ).rowcount
            conn.commit()
            return rowcount > 0

    def prune_facebook_posts(self, retention_days: int) -> list[dict[str, str]]:
        cutoff = _iso(_utcnow() - timedelta(days=retention_days))
        with self._lock, self.connect() as conn:
            rows = conn.execute(
                """
                SELECT screenshot_path, raw_snapshot_path
                FROM facebook_posts
                WHERE last_seen_at < ?
                """,
                (cutoff,),
            ).fetchall()
            conn.execute("DELETE FROM facebook_posts WHERE last_seen_at < ?", (cutoff,))
            conn.commit()
        return [dict(row) for row in rows]

    def list_jobs(
        self,
        role: str | None = None,
        source: str | None = None,
        location: str | None = None,
        early_career: bool | None = None,
        min_experience_score: float | None = None,
        new_since_hours: int | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        where: list[str] = []
        values: list[Any] = []

        if role:
            where.append("role_family = ?")
            values.append(role)
        if source:
            where.append("source = ?")
            values.append(source)
        if location:
            if location == "__remote_outside__":
                where.append("lower(location) LIKE '%remote%'")
                where.append("lower(location) NOT LIKE '%egypt%'")
                where.append("lower(location) NOT LIKE '%alexandria%'")
                where.append("lower(location) NOT LIKE '%cairo%'")
            else:
                where.append("location LIKE ?")
                values.append(f"%{location}%")
        if early_career is not None:
            where.append("is_early_career = ?")
            values.append(int(early_career))
        if min_experience_score is not None:
            where.append("early_career_score >= ?")
            values.append(min_experience_score)
        if new_since_hours is not None:
            cutoff = _iso(_utcnow() - timedelta(hours=new_since_hours))
            where.append("content_updated_at >= ?")
            values.append(cutoff)

        clause = f"WHERE {' AND '.join(where)}" if where else ""
        sql = f"""
        SELECT *
        FROM jobs
        {clause}
        ORDER BY {LOCATION_PRIORITY_SQL} ASC, role_priority DESC, early_career_score DESC, content_updated_at DESC
        LIMIT ?
        """
        values.append(limit)

        with self.connect() as conn:
            rows = conn.execute(sql, values).fetchall()
        return [dict(row) for row in rows]

    def list_digest_items(self, hours: int = 24, limit: int = 150) -> list[DigestItem]:
        cutoff = _iso(_utcnow() - timedelta(hours=hours))
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT title, company, location, role_family, early_career_score, apply_url, source,
                       posted_at, content_updated_at
                FROM jobs
                WHERE content_updated_at >= ?
                ORDER BY
                    CASE
                        WHEN lower(location) LIKE '%alexandria%' THEN 0
                        WHEN lower(location) LIKE '%cairo%' THEN 1
                        WHEN lower(location) LIKE '%remote%'
                             AND lower(location) NOT LIKE '%egypt%'
                             AND lower(location) NOT LIKE '%alexandria%'
                             AND lower(location) NOT LIKE '%cairo%' THEN 2
                        ELSE 3
                    END ASC,
                    role_priority DESC,
                    early_career_score DESC,
                    content_updated_at DESC
                LIMIT ?
                """,
                (cutoff, limit),
            ).fetchall()
        items: list[DigestItem] = []
        for row in rows:
            items.append(
                DigestItem(
                    title=row["title"],
                    company=row["company"],
                    location=row["location"],
                    role_family=row["role_family"],
                    early_career_score=row["early_career_score"],
                    apply_url=row["apply_url"],
                    source=row["source"],
                    posted_at=_parse(row["posted_at"]),
                    updated_at=_parse(row["content_updated_at"]) or _utcnow(),
                )
            )
        return items
