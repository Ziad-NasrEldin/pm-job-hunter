from __future__ import annotations

from typing import Any

import httpx

from app.config import Settings

try:
    from plyer import notification as plyer_notification
except Exception:  # noqa: BLE001
    plyer_notification = None


class FacebookAlertService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def notify_new_leads(self, *, new_count: int, run_id: int) -> dict[str, Any]:
        result = {
            "desktop": "skipped",
            "email": "skipped",
            "new_count": new_count,
            "run_id": run_id,
        }
        if new_count <= 0 or not self.settings.facebook_alerts_enabled:
            return result

        result["desktop"] = self._send_desktop(new_count=new_count, run_id=run_id)
        if self.settings.facebook_alert_email_enabled:
            result["email"] = self._send_email(new_count=new_count, run_id=run_id)
        return result

    def _send_desktop(self, *, new_count: int, run_id: int) -> str:
        if plyer_notification is None:
            return "unavailable"
        try:
            plyer_notification.notify(
                title="PM Job Hunter: New Facebook Leads",
                message=f"{new_count} new leads found in Facebook run #{run_id}.",
                timeout=8,
            )
            return "sent"
        except Exception:  # noqa: BLE001
            return "failed"

    def _send_email(self, *, new_count: int, run_id: int) -> str:
        if not self.settings.resend_api_key:
            return "missing_resend_api_key"
        if not self.settings.digest_from_email:
            return "missing_from_email"

        to_email = self.settings.facebook_alert_email_to or self.settings.digest_to_email
        if not to_email:
            return "missing_to_email"

        payload = {
            "from": self.settings.digest_from_email,
            "to": [to_email],
            "subject": f"Facebook lead alert: {new_count} new leads (run #{run_id})",
            "text": (
                f"PM Job Hunter found {new_count} new Facebook leads.\n"
                f"Run ID: {run_id}\n"
                "Open the dashboard and switch to Facebook tab for details."
            ),
        }
        headers = {
            "Authorization": f"Bearer {self.settings.resend_api_key}",
            "Content-Type": "application/json",
        }
        try:
            with httpx.Client(timeout=self.settings.request_timeout_seconds) as client:
                response = client.post("https://api.resend.com/emails", headers=headers, json=payload)
                response.raise_for_status()
            return "sent"
        except Exception:  # noqa: BLE001
            return "failed"
