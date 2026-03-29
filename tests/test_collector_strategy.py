from app.collector import JobCollector
from app.config import Settings
from app.db import Database


class QueryCaptureAdapter:
    source_name = "capture"

    def __init__(self, captured: dict):
        self.captured = captured

    def fetch_jobs(self, query):
        self.captured["locations"] = list(query.locations)
        return []

    def close(self):
        return None


def test_collector_prioritizes_alexandria_cairo_remote(tmp_path):
    db = Database(str(tmp_path / "jobs.db"))
    db.init()
    settings = Settings(
        db_path=str(tmp_path / "jobs.db"),
        enable_scheduler=False,
        mnea_locations=[
            "Egypt",
            "Remote",
            "Cairo",
            "Saudi Arabia",
            "Alexandria",
            "Qatar",
        ],
    )
    collector = JobCollector(settings, db)
    captured: dict = {}
    collector._build_adapters = lambda: [QueryCaptureAdapter(captured)]  # type: ignore[method-assign]

    collector.run_once()

    assert captured["locations"][:3] == ["Alexandria", "Cairo", "Remote"]
    assert captured["locations"][3:] == ["Egypt", "Saudi Arabia", "Qatar"]

