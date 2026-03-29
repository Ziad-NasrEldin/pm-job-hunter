from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx

from app.config import Settings
from app.db import Database
from app.models import DigestItem


class DigestService:
    def __init__(self, settings: Settings, db: Database) -> None:
        self.settings = settings
        self.db = db

    def _render_html(self, items: list[DigestItem]) -> str:
        rows = []
        for item in items:
            posted = item.posted_at.date().isoformat() if item.posted_at else "N/A"
            rows.append(
                (
                    "<tr>"
                    f"<td>{item.title}</td>"
                    f"<td>{item.company}</td>"
                    f"<td>{item.location}</td>"
                    f"<td>{item.role_family}</td>"
                    f"<td>{item.early_career_score:.2f}</td>"
                    f"<td>{posted}</td>"
                    f'<td><a href="{item.apply_url}">Apply</a></td>'
                    "</tr>"
                )
            )
        table_rows = "".join(rows)
        return (
            "<html><body>"
            "<h2>PM Job Hunter: New/Updated Jobs (last 24h)</h2>"
            "<table border='1' cellpadding='6' cellspacing='0'>"
            "<thead><tr>"
            "<th>Title</th><th>Company</th><th>Location</th><th>Role</th>"
            "<th>Early-Career Score</th><th>Posted</th><th>Link</th>"
            "</tr></thead>"
            f"<tbody>{table_rows}</tbody></table>"
            "</body></html>"
        )

    def _render_text(self, items: list[DigestItem]) -> str:
        lines = ["PM Job Hunter: New/Updated Jobs (last 24h)", ""]
        for item in items:
            lines.append(
                f"- {item.title} | {item.company} | {item.location} | "
                f"{item.role_family} | score={item.early_career_score:.2f}"
            )
            lines.append(f"  {item.apply_url}")
        return "\n".join(lines)

    def _can_send_email(self) -> bool:
        return bool(
            self.settings.resend_api_key
            and self.settings.digest_from_email
            and self.settings.digest_to_email
        )

    def send_daily_digest(self, hours: int = 24) -> dict[str, Any]:
        items = self.db.list_digest_items(hours=hours)
        if not items:
            return {"status": "no_items", "count": 0}
        if not self._can_send_email():
            return {"status": "missing_config", "count": len(items)}

        html = self._render_html(items)
        text = self._render_text(items)
        subject_date = datetime.now(UTC).date().isoformat()
        payload = {
            "from": self.settings.digest_from_email,
            "to": [self.settings.digest_to_email],
            "subject": f"PM Job Digest ({subject_date}) - {len(items)} matches",
            "html": html,
            "text": text,
        }
        headers = {
            "Authorization": f"Bearer {self.settings.resend_api_key}",
            "Content-Type": "application/json",
        }
        with httpx.Client(timeout=self.settings.request_timeout_seconds) as client:
            response = client.post("https://api.resend.com/emails", headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
        return {"status": "sent", "count": len(items), "response": data}

