# AI Chip Tech NewsHub

A global AI / semiconductor / technology news dashboard.

It combines:
- Worldwide tech RSS feeds filtered by AI/chip/tech keywords
- Tracked X account posts with fallback modes
- Bilingual display (English + Chinese) for news, X posts, and summary

## Features

- Multi-source RSS aggregation with keyword filtering
- In-page tracked X post reader with media and engagement metrics
- Automatic fallback chain: X API -> reader extraction -> timeline embed
- Real-time bilingual summary:
  - key developments
  - main players
  - hot themes
  - key metrics
- Flask API + single-page frontend

## Run locally

```bash
cd /Users/yimengli/ai-chip-tech-newshub
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 app.py
```

Optional:
```bash
export X_BEARER_TOKEN='YOUR_X_API_BEARER_TOKEN'
```

Open:
- `http://localhost:5053` for UI
- `http://localhost:5053/api/health` for health

## API endpoints

- `GET /api/news?limit=200&force=1&translate=1`
- `GET /api/sources`
- `GET /api/x-accounts`
- `GET /api/x-posts?handle=OpenAI&limit=10&force=1&translate=1`
- `GET /api/summary?force=1&x_limit=3`
- `GET /api/health`

## Config

- `PORT` (default `5052`)
- `CACHE_TTL_SECONDS` (default `300`)
- `LOOKBACK_DAYS` (default `14`)
- `FETCH_TIMEOUT_SECONDS` (default `20`)
- `MAX_ITEMS_PER_SOURCE` (default `60`)
- `X_BEARER_TOKEN` (optional, improves X API mode)
- `X_CACHE_TTL_SECONDS` (default `180`)
- `X_DEFAULT_POST_LIMIT` (default `8`)
- `X_READER_FALLBACK_ENABLED` (default `1`)
- `SUMMARY_CACHE_TTL_SECONDS` (default `90`)
- `SUMMARY_DEFAULT_X_LIMIT` (default `3`)
- `SUMMARY_TRANSLATION_ENABLED` (default `1`)

## Deploy to Render

This repo includes `render.yaml`.

Manual Web Service settings:
- Build Command: `pip install -r requirements.txt`
- Start Command: `gunicorn app:app --bind 0.0.0.0:$PORT --workers 2 --threads 4`
