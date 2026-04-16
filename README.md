# SLBM Tracker

NSE Securities Lending & Borrowing (SLB) tracking system for a family office. Monitors live SLB rate data, parses broker contract notes from email, maintains a transaction ledger, calculates brokerage refunds, and sends Telegram alerts.

## Quick Start

```bash
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your Telegram token, Gmail credentials, etc.
python main.py
```

The dashboard will be available at `http://localhost:8000`.

## Features

- **Live SLB Rate Polling** — fetches NSE SLBM data every 60s during market hours (Mon-Fri 9:15-15:30 IST)
- **Portfolio Tracking** — add/remove stocks via dashboard, only tracks bids for your stocks
- **Telegram Alerts** — morning summary, new bid alerts, rate threshold alerts, EOD summary
- **Lending Advisor** — compares current vs next month series yield to recommend optimal lending
- **Contract Note Parsing** — Gmail integration to auto-parse HDFC/ICICI broker PDFs
- **Brokerage Refund Tracker** — calculates difference between charged (15%) and deal rate (8%)
- **Transaction Ledger** — full history with filters, quarterly views, CSV export

## Series B (X-Series) Explanation

The system tracks **X-series SLB contracts** (X1 through XC), which are NSE's monthly non-foreclose series:

| Month | Code | | Month | Code |
|-------|------|-|-------|------|
| Jan   | X1   | | Jul   | X7   |
| Feb   | X2   | | Aug   | X8   |
| Mar   | X3   | | Sep   | X9   |
| Apr   | X4   | | Oct   | XA   |
| May   | X5   | | Nov   | XB   |
| Jun   | X6   | | Dec   | XC   |

It auto-calculates the current and next month series from today's date:

- If today is April → current series = `X4`, next series = `X5`
- SLB contracts expire on the last Thursday of each month
- The poller downloads the Market Watch CSV for each series separately (e.g. `MW-SLB-X5-07-Apr-2026.csv`)

Override in `.env` if needed:
```
CURRENT_SERIES=X5
NEXT_SERIES=X6
```

## Telegram Bot Setup

1. Message [@BotFather](https://t.me/BotFather) on Telegram → `/newbot`
2. Copy the bot token → set `TELEGRAM_BOT_TOKEN` in `.env`
3. Add the bot to your group chat
4. Get the chat ID:
   - Send a message in the group
   - Visit `https://api.telegram.org/bot<TOKEN>/getUpdates`
   - Find the `chat.id` value (negative number for groups)
5. Set `TELEGRAM_CHAT_ID` in `.env`

You can also configure Telegram via the Settings page in the dashboard.

## Gmail OAuth Setup

Required for automatic contract note parsing from email.

### 1. Create GCP Project
1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Create a new project (e.g. "SLBM Tracker")
3. Enable the **Gmail API** under APIs & Services → Library

### 2. Create OAuth Credentials
1. Go to APIs & Services → Credentials
2. Create OAuth 2.0 Client ID (Desktop application)
3. Download the JSON → note the `client_id` and `client_secret`

### 3. Generate Refresh Token
```python
from google_auth_oauthlib.flow import InstalledAppFlow

flow = InstalledAppFlow.from_client_config(
    {
        "installed": {
            "client_id": "YOUR_CLIENT_ID",
            "client_secret": "YOUR_CLIENT_SECRET",
            "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob"],
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    },
    scopes=["https://www.googleapis.com/auth/gmail.readonly"],
)
creds = flow.run_local_server(port=0)
print("Refresh Token:", creds.refresh_token)
```

4. Set in `.env`:
```
GMAIL_CLIENT_ID=your-client-id
GMAIL_CLIENT_SECRET=your-client-secret
GMAIL_REFRESH_TOKEN=your-refresh-token
GMAIL_EMAIL_ADDRESS=your@gmail.com
```

## Railway Deployment

1. Push this repo to GitHub
2. Go to [Railway](https://railway.app) → New Project → Deploy from GitHub repo
3. Set all environment variables from `.env.example`
4. Railway auto-detects the `Procfile` or `railway.toml` and deploys
5. Access the dashboard via the Railway-provided URL

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `TELEGRAM_BOT_TOKEN` | For alerts | - | Telegram bot token from BotFather |
| `TELEGRAM_CHAT_ID` | For alerts | - | Target chat/group ID |
| `GMAIL_CLIENT_ID` | For email | - | Google OAuth client ID |
| `GMAIL_CLIENT_SECRET` | For email | - | Google OAuth client secret |
| `GMAIL_REFRESH_TOKEN` | For email | - | OAuth refresh token |
| `GMAIL_EMAIL_ADDRESS` | For email | - | Gmail address to poll |
| `SECRET_KEY` | Yes | `change-me...` | App secret key |
| `DATABASE_PATH` | No | `data/slbm.db` | SQLite database path |
| `POLLING_INTERVAL_SECONDS` | No | `60` | SLB polling interval (30–300s) |
| `LOG_LEVEL` | No | `INFO` | Logging level |
| `BROKERAGE_DEAL_RATE` | No | `0.08` | Your deal rate with broker (8%) |
| `STT_RATE` | No | `0.0015` | STT rate (0.15% post Apr 2026) |

## Architecture

- **FastAPI** + Jinja2 templates — no separate frontend build
- **SQLite** — single file DB, no external dependencies
- **APScheduler** — in-process job scheduling
- All temp files (CSVs, PDFs) deleted immediately after parsing
- Portable: only env vars change between local/Railway/VM
