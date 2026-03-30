from app.config import Settings
from app.db import Database
from app.facebook_collector import FacebookCollector
from app.models import FacebookGroupCandidate


class FailingAdapter:
    source_name = "facebook_groups"

    def validate_session(self):
        return {"session_file_present": True, "session_valid": True, "reason": "ok"}

    def fetch_groups_posts(self, _groups):
        raise RuntimeError("adapter boom")


class CollectorWithFailingAdapter(FacebookCollector):
    def _build_adapter(self):
        return FailingAdapter()


class SessionInvalidAdapter:
    source_name = "facebook_groups"

    def validate_session(self):
        return {"session_file_present": True, "session_valid": False, "reason": "expired"}

    def fetch_groups_posts(self, _groups):
        return {}, {}


class CollectorWithInvalidSessionAdapter(FacebookCollector):
    def _build_adapter(self):
        return SessionInvalidAdapter()


def _settings(tmp_path):
    return Settings(
        db_path=str(tmp_path / "test.db"),
        enable_scheduler=False,
        facebook_enabled=True,
        facebook_storage_state_path=str(tmp_path / "state.json"),
    )


def test_collection_runtime_error_is_single_and_clear(tmp_path):
    settings = _settings(tmp_path)
    settings.ensure_runtime_dirs()
    db = Database(settings.db_path)
    db.init()

    candidate = FacebookGroupCandidate(
        group_external_id="group-1",
        name="Test Group",
        group_url="https://www.facebook.com/groups/group-1/",
        description="desc",
        relevance_score=0.9,
        discovered_keyword="test",
        metadata={},
    )
    db.upsert_facebook_group_candidate(candidate)
    db.approve_facebook_group("group-1")

    collector = CollectorWithFailingAdapter(settings, db)
    result = collector.run_once()

    assert result.status == "failed"
    assert len(result.errors) == 1
    assert result.errors[0] == "facebook_groups: adapter boom"


def test_collection_blocks_when_session_invalid(tmp_path):
    settings = _settings(tmp_path)
    settings.ensure_runtime_dirs()
    db = Database(settings.db_path)
    db.init()

    candidate = FacebookGroupCandidate(
        group_external_id="group-1",
        name="Test Group",
        group_url="https://www.facebook.com/groups/group-1/",
        description="desc",
        relevance_score=0.9,
        discovered_keyword="test",
        metadata={},
    )
    db.upsert_facebook_group_candidate(candidate)
    db.approve_facebook_group("group-1")

    collector = CollectorWithInvalidSessionAdapter(settings, db)
    result = collector.run_once()

    assert result.status == "blocked_session_expired"
    assert "expired" in result.errors[0]
