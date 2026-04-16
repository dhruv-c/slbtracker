"""
EOD Prices — fetches NSE Bhavcopy at 3:35 PM to store closing prices.
Retries at 4:00 PM and 4:30 PM if not yet available.
"""
import csv
import io
import logging
import zipfile
from datetime import date

import requests
import sqlalchemy as sa

from config.settings import settings
from db.database import execute, fetch_all
from db.models import daily_closes, portfolio
from jobs.slb_poller import get_nse_session

logger = logging.getLogger(__name__)

BHAVCOPY_URL_TEMPLATE = (
    "https://nsearchives.nseindia.com/content/cm/"
    "BhavCopy_NSE_CM_0_0_0_{date}_F_0000.csv.zip"
)


def fetch_bhavcopy(trade_date: date) -> list[dict] | None:
    """Download and parse the NSE bhavcopy zip for the given date."""
    date_str = trade_date.strftime("%d%m%Y")
    url = BHAVCOPY_URL_TEMPLATE.format(date=date_str)

    session = get_nse_session()
    try:
        resp = session.get(url, timeout=30)
        if resp.status_code != 200:
            logger.warning("Bhavcopy not available yet (HTTP %s)", resp.status_code)
            return None

        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as f:
            text = f.read().decode("utf-8")
            reader = csv.DictReader(io.StringIO(text))
            return list(reader)

    except Exception as e:
        logger.error("Failed to fetch bhavcopy: %s", e)
        return None


def store_eod_prices():
    """Main function called by scheduler. Fetches bhavcopy and stores closing prices."""
    today = date.today()
    logger.info("Fetching EOD prices for %s", today)

    data = fetch_bhavcopy(today)
    if data is None:
        logger.warning("Bhavcopy not available, will retry later")
        return False

    portfolio_symbols = {
        row.symbol.upper()
        for row in fetch_all(sa.select(portfolio.c.symbol).where(portfolio.c.active == 1))
    }

    if not portfolio_symbols:
        logger.info("No portfolio stocks, skipping EOD prices")
        return True

    # Column names in bhavcopy vary; try common ones
    symbol_cols = ["TckrSymb", "SYMBOL", "Symbol"]
    close_cols = ["ClsPric", "CLOSE", "Close"]
    prev_cols = ["PrvsClsgPric", "PREVCLOSE", "Prev Close"]

    inserted = 0
    for row in data:
        symbol = None
        for col in symbol_cols:
            if col in row:
                symbol = row[col].strip().upper()
                break
        if not symbol or symbol not in portfolio_symbols:
            continue

        close_price = None
        for col in close_cols:
            if col in row and row[col].strip():
                try:
                    close_price = float(row[col].strip())
                except ValueError:
                    pass
                break

        if close_price is None:
            continue

        prev_close = None
        for col in prev_cols:
            if col in row and row[col].strip():
                try:
                    prev_close = float(row[col].strip())
                except ValueError:
                    pass
                break

        change_pct = None
        if prev_close and prev_close > 0:
            change_pct = round((close_price - prev_close) / prev_close * 100, 2)

        # Upsert
        existing = fetch_all(
            sa.select(daily_closes)
            .where(daily_closes.c.trade_date == today)
            .where(daily_closes.c.symbol == symbol)
        )
        if existing:
            continue

        execute(
            daily_closes.insert().values(
                trade_date=today,
                symbol=symbol,
                close_price=close_price,
                prev_close=prev_close,
                change_pct=change_pct,
            )
        )
        inserted += 1

    logger.info("Stored %d EOD prices", inserted)
    return True
