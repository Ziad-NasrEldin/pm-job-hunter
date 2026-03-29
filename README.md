# PM Job Hunter (V2)

Local-first FastAPI app that aggregates:
- Product jobs from LinkedIn/Greenhouse/Lever.
- Remote Facebook group jobs (Arabic + English) with phone extraction and post screenshots.

## Features
- Existing PM pipeline: role ranking, early-career scoring, dedupe, CSV export.
- Dashboard split into 2 tabs:
  - `PM Search` (LinkedIn/Greenhouse/Lever pipeline)
  - `Facebook Scraper` (group discovery, approval, and remote-leads extraction)
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
  - global quick actions bar with `Run Facebook Scraper` and `Run Group Discovery`
  - one-click `Facebook Login` from dashboard (no terminal prompt)

## Quick Start
1. Create virtual environment and install dependencies:
   - `python -m venv .venv`
   - `.venv\Scripts\activate`
   - `pip install -r requirements.txt`
2. Install Playwright browser runtime once:
   - `playwright install chromium`
3. Copy `.env.local.example` to `.env.local` and set values.
   - Keep `FACEBOOK_HEADLESS=false` for more reliable discovery/crawling.
4. Bootstrap Facebook login session once:
   - Recommended: use dashboard quick action `Facebook Login` (opens browser and waits for login automatically).
   - CLI fallback: `python -m app.cli facebook-login`
   - Session is saved to `FACEBOOK_STORAGE_STATE_PATH`, used by discovery/collection runs.
5. Start app:
   - `uvicorn app.main:app --reload`
6. Open dashboard:
   - `http://127.0.0.1:8000/`
   - Use the `PM Search` and `Facebook Scraper` tabs to switch between pipelines.

## Windows EXE (Easy Launch)
1. From project root, build the executable:
   - `powershell -ExecutionPolicy Bypass -File .\scripts\build_exe.ps1`
2. Launch:
   - `.\dist\PMJobHunter.exe`
3. The app opens your browser automatically at:
   - `http://127.0.0.1:<port>/?tab=pm`

Notes:
- In installed/frozen mode, runtime data uses `%LOCALAPPDATA%\PMJobHunter\...`.
- If `%LOCALAPPDATA%\PMJobHunter\.env.local` is missing, it is auto-created from `.env.local.example`.
- EXE build is console-less (`--noconsole`) so no terminal window appears for end users.

## Windows Installer (Shareable)
1. Build installer locally:
   - `powershell -ExecutionPolicy Bypass -File .\scripts\build_installer.ps1 -Version v0.2.0`
2. Output:
   - `.\dist\PMJobHunter-Setup.exe`
3. Installer includes:
   - `PMJobHunter.exe`
   - `.env.local.example`
   - Playwright Chromium runtime under `%LOCALAPPDATA%\PMJobHunter\ms-playwright`

## GitHub Release Pipeline
- Tag push `v*` triggers `.github/workflows/release-windows.yml`.
- Pipeline steps:
  - run tests
  - build portable EXE
  - build installer EXE
  - generate `SHA256SUMS.txt`
  - publish assets to GitHub Release
- Artifacts:
  - `PMJobHunter.exe`
  - `PMJobHunter-Setup.exe`
  - `env.local.example`
  - `SHA256SUMS.txt`

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
- `GET /facebook/status`
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
