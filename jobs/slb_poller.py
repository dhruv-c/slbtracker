"""
SLB Poller — fetches NSE SLBM Market Watch CSVs (one per X-series)
every 60s during market hours, parses them, stores portfolio-relevant
rows, and triggers new-bid alerts.

Actual NSE MW CSV columns (after stripping whitespace):
  SYMBOL, BEST BID QTY, BEST BID PRICE, BEST OFFERS PRICE, BEST OFFERS QTY,
  LTP, UNDERLYING LTP, FUTURES LTP, SPREAD, SPREAD (%), OPEN POSITIONS,
  ANNUALISED YIELD (% p.a), VOLUME, TURNOVER (in ₹), TRANSACTION VALUE (in ₹), CA

Note: there is NO series column — the series is implicit from which CSV file
you download (one file per X-series).
"""
import csv
import io
import logging
import re
import time
from datetime import datetime

import requests
import sqlalchemy as sa

from config.settings import settings
from db.database import execute, fetch_all, fetch_one
from db.models import portfolio, slb_snapshots

logger = logging.getLogger(__name__)

# --- NSE Session Management ---

_nse_session: requests.Session | None = None
_session_created_at: float = 0
SESSION_MAX_AGE = 1800  # refresh cookies every 30 min

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/market-data/securities-lending-and-borrowing",
}

# NSE live SLB market watch JSON endpoint.
# This is what the SLB page itself calls to render the live market watch table.
# Returns JSON: {"data": [{symbol, buyOrderPrice1, buyOrderQty1, ...}, ...]}
SLB_MW_CSV_URL = "https://www.nseindia.com/api/live-analysis-slb"

# Fallback: direct archive URL pattern  (MW-SLB-{series}-{dd-Mon-yyyy}.csv)
SLB_ARCHIVE_URL = (
    "https://nsearchives.nseindia.com/content/slb/MW-SLB-{series}-{date}.csv"
)

# ── Column name mappings ─────────────────────────────────────────────
# The MW CSV header names have trailing whitespace/newlines which we strip
# during parsing.  After stripping, the actual column names from the sample
# file are listed first; legacy/API variants follow.
COLUMN_MAP = {
    "symbol":           ["symbol", "SYMBOL", "Symbol"],
    "best_bid_qty":     ["buyOrderQty1", "BEST BID QTY", "Best Bid Qty", "bestBidQty"],
    "best_bid_price":   ["buyOrderPrice1", "BEST BID PRICE", "Best Bid Price", "bestBidPrice"],
    "best_offer_price": ["sellOrderPrice1", "BEST OFFERS PRICE", "Best Offers Price", "bestOfferPrice"],
    "best_offer_qty":   ["sellQty1", "BEST OFFERS QTY", "Best Offers Qty", "bestOfferQty"],
    "ltp":              ["lastTradedPrice", "LTP", "ltp", "lastPrice", "Last Price"],
    "underlying_ltp":   ["underLyingLtp", "UNDERLYING LTP", "Underlying LTP", "underlyingValue"],
    "futures_ltp":      ["futuresLtp", "FUTURES LTP", "Futures LTP", "futuresLTP"],
    "spread":           ["spread", "SPREAD", "Spread"],
    "spread_pct":       ["spreadPer", "SPREAD (%)", "Spread (%)", "spreadPct"],
    "open_positions":   ["openPositions", "OPEN POSITIONS", "Open Positions", "openInterest"],
    "annualised_yield": ["annualisedYieldPer", "ANNUALISED YIELD (% p.a)", "ANNUALISED YIELD",
                         "Annualised Yield", "annualisedYield"],
    "volume":           ["volume", "VOLUME", "Volume", "totalTradedQty"],
    "turnover":         ["turnOver", "TURNOVER (in \u20b9)", "TURNOVER", "Turnover", "turnover"],
    "transaction_value":["transactionValue", "TRANSACTION VALUE (in \u20b9)", "TRANSACTION VALUE",
                         "Transaction Value"],
    "ca":               ["caExpDate", "CA", "Ca", "Corporate Action"],
    "allow_recall":     ["AllowRecall", "ALLOWRECALL", "allowRecall"],
    "allow_repay":      ["AllowRepay", "ALLOWREPAY", "allowRepay"],
}


def get_nse_session() -> requests.Session:
    """Get or refresh the NSE session with valid cookies."""
    global _nse_session, _session_created_at

    if _nse_session and (time.time() - _session_created_at) < SESSION_MAX_AGE:
        return _nse_session

    logger.info("Creating new NSE session...")
    session = requests.Session()
    session.headers.update(NSE_HEADERS)

    try:
        session.get("https://www.nseindia.com", timeout=15)
        session.get(
            "https://www.nseindia.com/market-data/securities-lending-and-borrowing",
            timeout=15,
        )
    except requests.RequestException as e:
        logger.warning("NSE session warmup failed: %s", e)

    _nse_session = session
    _session_created_at = time.time()
    return session


def _resolve_column(row: dict, field: str) -> str | None:
    """Try multiple column name variants to find a value."""
    for variant in COLUMN_MAP.get(field, [field]):
        if variant in row:
            return row[variant]
    return None


def _safe_float(val) -> float | None:
    if val is None or val == "" or val == "-":
        return None
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> int | None:
    if val is None or val == "" or val == "-":
        return None
    try:
        return int(float(str(val).replace(",", "").strip()))
    except (ValueError, TypeError):
        return None


def _parse_csv_text(text: str) -> list[dict]:
    """
    Parse NSE Market Watch CSV text.

    The NSE CSV has headers with embedded newlines inside quotes, e.g.:
        "SYMBOL \\n","BEST BID QTY \\n","ANNUALISED YIELD \\n(% p.a)",...

    Standard csv.DictReader handles this correctly when given the raw text
    (not pre-split by lines), because the csv module respects quoting.
    We then normalise the header names by collapsing whitespace.
    """
    # Strip BOM
    text = text.lstrip("\ufeff")

    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        return []

    # Normalise headers: collapse internal whitespace/newlines → single space, strip
    clean_fieldnames = [re.sub(r"\s+", " ", fn).strip() for fn in reader.fieldnames]
    reader.fieldnames = clean_fieldnames

    rows = []
    for raw_row in reader:
        row = {}
        for k, v in raw_row.items():
            if k is None:
                continue  # overflow columns
            clean_k = re.sub(r"\s+", " ", k).strip() if k else k
            if isinstance(v, str):
                row[clean_k] = v.strip().strip('"')
            else:
                row[clean_k] = v
        rows.append(row)

    return rows


def fetch_slb_data_for_series(series: str) -> list[dict] | None:
    """
    Fetch the Market Watch CSV for a single X-series from NSE.
    Tries the live API first, then falls back to the archive URL.
    """
    session = get_nse_session()

    # --- Attempt 1: live API endpoint ---
    try:
        resp = session.get(
            SLB_MW_CSV_URL,
            params={"series": series},
            timeout=20,
        )
        if resp.status_code == 200 and len(resp.text.strip()) > 50:
            content_type = resp.headers.get("Content-Type", "")

            # JSON response
            if "json" in content_type or resp.text.strip().startswith(("{", "[")):
                data = resp.json()
                if isinstance(data, dict):
                    data = data.get("data", data.get("rows", []))
                if isinstance(data, list) and data:
                    return data

            # CSV response
            rows = _parse_csv_text(resp.text)
            if rows:
                return rows

        logger.debug("Live API for %s returned %s", series, resp.status_code)
    except Exception as e:
        logger.warning("Live API fetch for %s failed: %s", series, e)

    # --- Attempt 2: archive URL ---
    try:
        date_str = datetime.now().strftime("%d-%b-%Y")  # e.g. "08-Apr-2026"
        url = SLB_ARCHIVE_URL.format(series=series, date=date_str)
        resp = session.get(url, timeout=20)
        if resp.status_code == 200 and len(resp.text.strip()) > 50:
            rows = _parse_csv_text(resp.text)
            if rows:
                return rows
        logger.debug("Archive URL for %s returned %s", series, resp.status_code)
    except Exception as e:
        logger.warning("Archive fetch for %s failed: %s", series, e)

    return None


def get_portfolio_symbols() -> set[str]:
    """Get active portfolio symbols."""
    rows = fetch_all(
        sa.select(portfolio.c.symbol).where(portfolio.c.active == 1)
    )
    return {row.symbol.upper() for row in rows}


def get_previous_snapshot(symbol: str, series: str):
    """Get the most recent snapshot for a symbol+series to detect new bids."""
    return fetch_one(
        sa.select(slb_snapshots)
        .where(slb_snapshots.c.symbol == symbol)
        .where(slb_snapshots.c.series == series)
        .order_by(slb_snapshots.c.snapshot_time.desc())
        .limit(1)
    )


def _process_rows(rows: list[dict], series: str, series_type: str,
                   portfolio_symbols: set[str], now: datetime) -> tuple[int, list[dict]]:
    """Process parsed CSV rows for one series. Returns (inserted_count, new_bids)."""
    inserted = 0
    new_bids = []

    for row in rows:
        symbol = (_resolve_column(row, "symbol") or "").strip().strip('"').upper()
        if not symbol:
            continue

        bid_qty = _safe_int(_resolve_column(row, "best_bid_qty"))
        bid_price = _safe_float(_resolve_column(row, "best_bid_price"))
        annualised = _safe_float(_resolve_column(row, "annualised_yield"))

        # Detect new bid (only for portfolio symbols — alerts are portfolio-only)
        is_new_bid = False
        if symbol in portfolio_symbols:
            prev = get_previous_snapshot(symbol, series)
            has_bid = bid_qty is not None and bid_qty > 0
            had_no_bid = (
                prev is None
                or prev.best_bid_qty is None
                or prev.best_bid_qty == 0
            )
            is_new_bid = has_bid and had_no_bid

        # NNF protocol flags (may not be in CSV — None if absent)
        allow_recall = _safe_int(_resolve_column(row, "allow_recall"))
        allow_repay = _safe_int(_resolve_column(row, "allow_repay"))

        snapshot_data = {
            "snapshot_time": now,
            "symbol": symbol,
            "series": series,
            "series_type": series_type,
            "best_bid_qty": bid_qty,
            "best_bid_price": bid_price,
            "best_offer_price": _safe_float(_resolve_column(row, "best_offer_price")),
            "best_offer_qty": _safe_int(_resolve_column(row, "best_offer_qty")),
            "ltp": _safe_float(_resolve_column(row, "ltp")),
            "underlying_ltp": _safe_float(_resolve_column(row, "underlying_ltp")),
            "futures_ltp": _safe_float(_resolve_column(row, "futures_ltp")),
            "spread": _safe_float(_resolve_column(row, "spread")),
            "spread_pct": _safe_float(_resolve_column(row, "spread_pct")),
            "open_positions": _safe_int(_resolve_column(row, "open_positions")),
            "annualised_yield_pct": annualised,
            "volume": _safe_int(_resolve_column(row, "volume")),
            "turnover_inr": _safe_float(_resolve_column(row, "turnover")),
            "transaction_value_inr": _safe_float(_resolve_column(row, "transaction_value")),
            "ca_date": (_resolve_column(row, "ca") or "").strip() or None,
            "allow_recall": allow_recall,
            "allow_repay": allow_repay,
        }

        execute(slb_snapshots.insert().values(**snapshot_data))
        inserted += 1

        if is_new_bid:
            new_bids.append(snapshot_data)

    return inserted, new_bids


def poll_slb_rates():
    """
    Main polling function — called by scheduler every 60s during market hours.
    Downloads the Market Watch CSV for each active X-series, filters for
    portfolio stocks, stores snapshots, and fires new-bid alerts.
    """
    logger.info("Polling SLB rates...")

    portfolio_symbols = get_portfolio_symbols()
    current_series, next_series = settings.get_active_series()
    now = datetime.now()
    total_inserted = 0
    all_new_bids: list[dict] = []

    for series, s_type in [(current_series, "current"), (next_series, "next")]:
        data = fetch_slb_data_for_series(series)
        if data is None:
            logger.warning("No SLB data for series %s this cycle", series)
            continue

        inserted, new_bids = _process_rows(data, series, s_type, portfolio_symbols, now)
        total_inserted += inserted
        all_new_bids.extend(new_bids)
        logger.info("Series %s: %d rows from CSV, %d stored, %d new bids",
                     series, len(data), inserted, len(new_bids))

    logger.info("SLB poll complete: %d rows inserted, %d new bids",
                total_inserted, len(all_new_bids))

    if all_new_bids:
        from jobs.alert_engine import send_new_bid_alerts
        send_new_bid_alerts(all_new_bids)
