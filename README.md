# PM Job Hunter

A local-first job hunter that ranks product roles and pulls remote leads out of Facebook groups — on your machine, not a cloud inbox.

This is a **personal localhost tool**, not a hosted product. Bind it to `127.0.0.1` only. Facebook login stores session cookies on disk; scraping groups is against Facebook's terms of service and collects other people's phone numbers. Keep `FACEBOOK_ENABLED=false` and `ENABLE_SCHEDULER=false` unless you know you want those on.

Built for product-management job seekers (and anyone collecting remote Arabic/English group posts) who want one dashboard instead of ten tabs.

- Rank LinkedIn, Greenhouse, and Lever product roles with early-career scoring and dedupe
- Discover Egypt-relevant Facebook groups, approve them, then crawl on a schedule
- Extract remote/WFH posts with phones, WhatsApp links, and category tags
- Export either pipeline to CSV, with screenshots and raw HTML kept locally
- Run it as a FastAPI dashboard or a Windows EXE / installer

## Run locally

Needs Python 3.11+ and Playwright Chromium. There is no public site on this repo. Do not expose the dashboard on the internet.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
cp .env.local.example .env.local
uvicorn app.main:app --reload
```

Open [http://127.0.0.1:8000/](http://127.0.0.1:8000/). Use the PM Search and Facebook Scraper tabs.

## Windows (share with a friend)

Download the latest **PMJobHunter-Setup.exe** from [Releases](https://github.com/Ziad-NasrEldin/pm-job-hunter/releases).

The installer is unsigned, so Windows SmartScreen may say “Windows protected your PC”. Click **More info** → **Run anyway**. The app stays on this computer and opens `http://127.0.0.1/...` in the browser. Facebook scraping is off until they turn it on in `%LOCALAPPDATA%\PMJobHunter\.env.local`.

Engineer notes (CLI, API, local Python) are in [`docs/local-setup.md`](docs/local-setup.md).

## How it works

FastAPI + SQLite + APScheduler. One collector hits public job boards; a second Playwright pipeline logs into Facebook once, then crawls approved groups. Everything stays under ./data (or LOCALAPPDATA/PMJobHunter in the frozen Windows build). Facebook markup and group visibility will make or break a crawl.

---

Built by [Ziad Ahmed](https://github.com/Ziad-NasrEldin) at [MaVoid](https://mavoid.com).

[Website](https://mavoid.com) · [LinkedIn](https://linkedin.com/in/ziad-ahmed-634202332) · [GitHub](https://github.com/Ziad-NasrEldin)
