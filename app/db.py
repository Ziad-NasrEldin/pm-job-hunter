from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from hashlib import sha1
from threading import Lock
from typing import Any

from app.models import DigestItem, RunSummary, ScoredJob


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
                CREATE INDEX IF NOT EXISTS idx_jobs_role ON jobs(role_priority DESC, early_career_score DESC);
                CREATE INDEX IF NOT EXISTS idx_jobs_updated ON jobs(content_updated_at DESC);
                """
            )
            conn.commit()

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
