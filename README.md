# PM Job Hunter (V1)

Local-first FastAPI app that aggregates early-career Product jobs for MENA + Remote.

## Features
- Public source adapters: LinkedIn public Jobs pages, Greenhouse, Lever.
- Role priority: Product Owner > Product Manager > APM.
- Loose early-career ranking with senior-role exclusion.
- SQLite persistence with deduplication.
- Daily collection (09:00 Africa/Cairo) and digest email (09:15 Africa/Cairo).
- Minimal dashboard with filters, manual run, and CSV export.

## Quick Start
1. Create a virtual environment and install dependencies:
   - `python -m venv .venv`
   - `.venv\Scripts\activate`
   - `pip install -r requirements.txt`
2. Copy `.env.example` to `.env` and fill:
   - `RESEND_API_KEY`
   - `DIGEST_FROM_EMAIL`
   - `DIGEST_TO_EMAIL`
3. Run the app:
   - `uvicorn app.main:app --reload`
4. Open dashboard:
   - `http://127.0.0.1:8000/`

## API
- `POST /runs/manual`
- `GET /runs/latest`
- `GET /jobs`
- `GET /jobs/export.csv`

## Notes
- LinkedIn adapter only uses public pages and does not require login.
- Source structures may change over time; parser failures are isolated per source.
- For stronger coverage, configure `GREENHOUSE_BOARDS` and `LEVER_COMPANIES`.
