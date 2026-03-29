# PM Job Hunter (V2)

Local-first FastAPI app that aggregates:
- Product jobs from LinkedIn/Greenhouse/Lever.
- Remote Facebook group jobs (Arabic + English) with phone extraction and post screenshots.

## Features
- Existing PM pipeline: role ranking, early-career scoring, dedupe, CSV export.
- Facebook discovery pipeline:
  - semi-automatic Egypt-relevant group discovery
  - pending group approval before crawling
  - crawl of approved groups every 2 hours (configurable)
- Facebook remote-job extraction:
  - strict remote/work-from-home filtering in Arabic + English
  - phone extraction (Arabic-Indic digits supported)
  - WhatsApp link extraction
  - category tagging (cold calling, sales, support, data entry, other)
  - post-card screenshot + raw HTML snapshot storage
- Dashboard support for:
  - approving/disabling groups
  - filtering Facebook leads
  - screenshot preview/open
  - CSV export for Facebook leads

## Quick Start
1. Create virtual environment and install dependencies:
   - `python -m venv .venv`
   - `.venv\Scripts\activate`
   - `pip install -r requirements.txt`
2. Install Playwright browser runtime once:
   - `playwright install chromium`
3. Copy `.env.local.example` to `.env.local` and set values.
   - Keep `FACEBOOK_HEADLESS=false` for more reliable discovery/crawling.
4. Bootstrap Facebook login session once (opens browser):
   - `python -m app.cli facebook-login`
   - Log in with your account, then press Enter in terminal.
   - This saves `FACEBOOK_STORAGE_STATE_PATH`, used by discovery/collection runs.
5. Start app:
   - `uvicorn app.main:app --reload`
6. Open dashboard:
   - `http://127.0.0.1:8000/`

## CLI Commands
- `python -m app.cli collect`
- `python -m app.cli digest`
- `python -m app.cli facebook-login`
- `python -m app.cli facebook-discover`
- `python -m app.cli facebook-collect`

## API
### PM Jobs
- `POST /runs/manual`
- `GET /runs/latest`
- `GET /jobs`
- `GET /jobs/export.csv`

### Facebook Jobs
- `POST /facebook/login/bootstrap`
- `POST /facebook/discovery/run`
- `GET /facebook/groups/candidates`
- `POST /facebook/groups/{group_id}/approve`
- `POST /facebook/groups/{group_id}/disable`
- `POST /facebook/runs/manual`
- `GET /facebook/runs/latest`
- `GET /facebook/posts`
- `GET /facebook/posts/export.csv`

## Storage Layout
- SQLite DB: `DB_PATH` (default `./data/jobs.db`)
- Screenshots: `./data/screenshots/facebook`
- Raw post snapshots: `./data/raw/facebook`
- Playwright profile/session: `./data/facebook_profile`

## Notes
- Facebook scraping reliability depends on current Facebook markup and account visibility permissions.
- Private/inaccessible groups are skipped.
- Existing PM job pipeline remains intact and separate from Facebook tables.
