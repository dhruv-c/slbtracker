"""Dashboard routes — main view, ledger, analytics, settings."""
import logging
from datetime import date, datetime, timedelta

import sqlalchemy as sa
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from config.settings import settings
from db.database import fetch_all, fetch_one, get_setting, set_setting, execute
from db.models import (
    alert_log,
    alert_thresholds,
    brokerage_refunds,
    portfolio,
    slb_snapshots,
    transactions,
)
from jobs.alert_engine import compare_series, _days_remaining_in_series

logger = logging.getLogger(__name__)
router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    """Main dashboard — active bids, alerts, quick stats."""
    current_series, next_series = settings.get_active_series()

    # Active bids for portfolio stocks
    symbols = fetch_all(sa.select(portfolio).where(portfolio.c.active == 1))
    active_bids = []
    no_bids = []

    for sym_row in symbols:
        sym = sym_row.symbol
        has_bid = False
        for series in [current_series, next_series]:
            snap = fetch_one(
                sa.select(slb_snapshots)
                .where(slb_snapshots.c.symbol == sym)
                .where(slb_snapshots.c.series == series)
                .where(slb_snapshots.c.best_bid_qty.isnot(None))
                .where(slb_snapshots.c.best_bid_qty > 0)
                .order_by(slb_snapshots.c.snapshot_time.desc())
                .limit(1)
            )
            if snap:
                active_bids.append({
                    "symbol": sym,
                    "series": series,
                    "series_type": "current" if series == current_series else "next",
                    "yield_pct": snap.annualised_yield_pct,
                    "bid_price": snap.best_bid_price,
                    "bid_qty": snap.best_bid_qty,
                    "spread_pct": snap.spread_pct,
                    "underlying": snap.underlying_ltp,
                    "time": snap.snapshot_time,
                    "annualised_yield_pct": snap.annualised_yield_pct,
                })
                has_bid = True
        if not has_bid:
            no_bids.append(sym)

    # Lending advisor — compare series for stocks with bids in both
    advisor_data = []
    symbols_with_both = set()
    for bid in active_bids:
        symbols_with_both.add(bid["symbol"])

    for sym in symbols_with_both:
        current_bid = next((b for b in active_bids if b["symbol"] == sym and b["series"] == current_series), None)
        next_bid = next((b for b in active_bids if b["symbol"] == sym and b["series"] == next_series), None)
        if current_bid and next_bid:
            curr_days = _days_remaining_in_series(current_series)
            next_days = _days_remaining_in_series(next_series)
            comp = compare_series(current_bid, next_bid, curr_days, next_days)
            if comp:
                advisor_data.append({
                    "symbol": sym,
                    "current_yield_pct": current_bid["yield_pct"],
                    "current_bid_qty": current_bid["bid_qty"],
                    "next_yield_pct": next_bid["yield_pct"],
                    "next_bid_qty": next_bid["bid_qty"],
                    "current_days": curr_days,
                    "next_days": next_days,
                    **comp,
                })

    # Recent alerts
    alerts = fetch_all(
        sa.select(alert_log)
        .order_by(alert_log.c.alert_time.desc())
        .limit(20)
    )

    # Quick stats
    open_positions = fetch_one(
        sa.select(sa.func.count()).select_from(transactions)
        .where(transactions.c.transaction_type == "LEND")
    )

    today_income = fetch_one(
        sa.select(sa.func.coalesce(sa.func.sum(transactions.c.net_income), 0))
        .where(transactions.c.trade_date == date.today())
    )

    pending_refund = fetch_one(
        sa.select(
            sa.func.coalesce(sa.func.sum(brokerage_refunds.c.total_refund_due), 0)
            - sa.func.coalesce(sa.func.sum(brokerage_refunds.c.total_refund_received), 0)
        )
        .where(brokerage_refunds.c.status != "SETTLED")
    )

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "active_bids": active_bids,
        "no_bids": no_bids,
        "advisor_data": advisor_data,
        "alerts": alerts,
        "current_series": current_series,
        "next_series": next_series,
        "stats": {
            "open_positions": open_positions[0] if open_positions else 0,
            "today_income": today_income[0] if today_income else 0,
            "pending_refund": pending_refund[0] if pending_refund else 0,
        },
    })


@router.get("/ledger", response_class=HTMLResponse)
def ledger(request: Request, symbol: str = "", quarter: str = "", start: str = "", end: str = ""):
    """Transaction ledger with filters."""
    query = sa.select(transactions).order_by(transactions.c.trade_date.desc())

    if symbol:
        query = query.where(transactions.c.symbol == symbol.upper())
    if start:
        query = query.where(transactions.c.trade_date >= start)
    if end:
        query = query.where(transactions.c.trade_date <= end)
    if quarter:
        # Parse quarter like Q1-FY26 → Apr-Jun 2025
        q_map = _quarter_date_range(quarter)
        if q_map:
            query = query.where(transactions.c.trade_date >= q_map[0])
            query = query.where(transactions.c.trade_date <= q_map[1])

    txns = fetch_all(query)

    # Get all symbols for filter dropdown
    all_symbols = fetch_all(sa.select(portfolio.c.symbol).where(portfolio.c.active == 1))

    # Summary totals
    total_gross = sum(t.gross_income or 0 for t in txns)
    total_net = sum(t.net_income or 0 for t in txns)
    total_refund = sum(t.brokerage_refund_due or 0 for t in txns)

    return templates.TemplateResponse("ledger.html", {
        "request": request,
        "transactions": txns,
        "symbols": [s.symbol for s in all_symbols],
        "filters": {"symbol": symbol, "quarter": quarter, "start": start, "end": end},
        "totals": {"gross": total_gross, "net": total_net, "refund": total_refund},
    })


@router.get("/analytics", response_class=HTMLResponse)
def analytics(request: Request):
    """Analytics — brokerage refund tracker, income P&L, lending advisor."""
    # Brokerage refunds by quarter
    refunds = fetch_all(
        sa.select(brokerage_refunds).order_by(brokerage_refunds.c.quarter.desc())
    )

    # Monthly income
    monthly_income = fetch_all(
        sa.select(
            sa.func.strftime("%Y-%m", transactions.c.trade_date).label("month"),
            sa.func.sum(transactions.c.gross_income).label("gross"),
            sa.func.sum(transactions.c.net_income).label("net"),
            sa.func.count().label("count"),
        )
        .group_by(sa.func.strftime("%Y-%m", transactions.c.trade_date))
        .order_by(sa.func.strftime("%Y-%m", transactions.c.trade_date).desc())
    )

    # Per-stock summary
    stock_summary = fetch_all(
        sa.select(
            transactions.c.symbol,
            sa.func.count().label("txn_count"),
            sa.func.sum(transactions.c.gross_income).label("total_gross"),
            sa.func.sum(transactions.c.net_income).label("total_net"),
            sa.func.sum(transactions.c.brokerage_refund_due).label("total_refund"),
        )
        .group_by(transactions.c.symbol)
        .order_by(sa.func.sum(transactions.c.net_income).desc())
    )

    # Lending advisor
    current_series, next_series = settings.get_active_series()
    advisor_data = _build_advisor_data(current_series, next_series)

    return templates.TemplateResponse("analytics.html", {
        "request": request,
        "refunds": refunds,
        "monthly_income": monthly_income,
        "stock_summary": stock_summary,
        "advisor_data": advisor_data,
        "current_series": current_series,
        "next_series": next_series,
    })


@router.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request):
    """Settings page — Telegram, thresholds, series config."""
    thresholds = fetch_all(sa.select(alert_thresholds))
    symbols = fetch_all(sa.select(portfolio.c.symbol).where(portfolio.c.active == 1))
    current_series, next_series = settings.get_active_series()

    return templates.TemplateResponse("settings.html", {
        "request": request,
        "telegram_bot_token": get_setting("telegram_bot_token", settings.telegram_bot_token),
        "telegram_chat_id": get_setting("telegram_chat_id", settings.telegram_chat_id),
        "thresholds": thresholds,
        "symbols": [s.symbol for s in symbols],
        "polling_interval": settings.polling_interval_seconds,
        "current_series": current_series,
        "next_series": next_series,
        "gmail_configured": bool(settings.gmail_refresh_token),
    })


@router.post("/settings/telegram")
def save_telegram_settings(bot_token: str = Form(""), chat_id: str = Form("")):
    """Save Telegram settings to DB."""
    set_setting("telegram_bot_token", bot_token)
    set_setting("telegram_chat_id", chat_id)
    return {"status": "ok"}


@router.post("/settings/threshold")
def save_threshold(symbol: str = Form(...), min_rate: float = Form(...)):
    """Save or update rate threshold for a symbol."""
    existing = fetch_one(
        sa.select(alert_thresholds).where(alert_thresholds.c.symbol == symbol)
    )
    if existing:
        execute(
            alert_thresholds.update()
            .where(alert_thresholds.c.symbol == symbol)
            .values(min_rate=min_rate)
        )
    else:
        execute(alert_thresholds.insert().values(symbol=symbol, min_rate=min_rate))
    return {"status": "ok"}


@router.post("/settings/threshold/delete")
def delete_threshold(symbol: str = Form(...)):
    execute(alert_thresholds.delete().where(alert_thresholds.c.symbol == symbol))
    return {"status": "ok"}


@router.post("/analytics/refund/received")
def mark_refund_received(quarter: str = Form(...), amount: float = Form(...)):
    """Mark partial or full refund as received."""
    refund = fetch_one(
        sa.select(brokerage_refunds).where(brokerage_refunds.c.quarter == quarter)
    )
    if not refund:
        return {"error": "Quarter not found"}

    new_received = (refund.total_refund_received or 0) + amount
    new_status = "SETTLED" if new_received >= (refund.total_refund_due or 0) else "PARTIAL"

    execute(
        brokerage_refunds.update()
        .where(brokerage_refunds.c.quarter == quarter)
        .values(
            total_refund_received=new_received,
            status=new_status,
        )
    )
    return {"status": "ok", "new_received": new_received, "new_status": new_status}


# --- Helpers ---

def _quarter_date_range(quarter: str):
    """Parse 'Q1-FY26' → (date start, date end). FY starts April."""
    import re
    m = re.match(r"Q(\d)-FY(\d{2})", quarter)
    if not m:
        return None
    q = int(m.group(1))
    fy = 2000 + int(m.group(2))
    # FY26 means April 2025 to March 2026
    month_starts = {1: (4, fy - 1), 2: (7, fy - 1), 3: (10, fy - 1), 4: (1, fy)}
    if q not in month_starts:
        return None
    start_month, start_year = month_starts[q]
    end_month = start_month + 2
    end_year = start_year
    if end_month > 12:
        end_month -= 12
        end_year += 1
    import calendar
    last_day = calendar.monthrange(end_year, end_month)[1]
    return (date(start_year, start_month, 1), date(end_year, end_month, last_day))


def _build_advisor_data(current_series, next_series):
    """Build lending advisor comparison for all stocks with bids in both series."""
    symbols = fetch_all(sa.select(portfolio.c.symbol).where(portfolio.c.active == 1))
    advisor = []

    for sym_row in symbols:
        sym = sym_row.symbol
        current_snap = fetch_one(
            sa.select(slb_snapshots)
            .where(slb_snapshots.c.symbol == sym)
            .where(slb_snapshots.c.series == current_series)
            .where(slb_snapshots.c.best_bid_qty.isnot(None))
            .where(slb_snapshots.c.best_bid_qty > 0)
            .order_by(slb_snapshots.c.snapshot_time.desc())
            .limit(1)
        )
        next_snap = fetch_one(
            sa.select(slb_snapshots)
            .where(slb_snapshots.c.symbol == sym)
            .where(slb_snapshots.c.series == next_series)
            .where(slb_snapshots.c.best_bid_qty.isnot(None))
            .where(slb_snapshots.c.best_bid_qty > 0)
            .order_by(slb_snapshots.c.snapshot_time.desc())
            .limit(1)
        )

        if current_snap and next_snap:
            curr_days = _days_remaining_in_series(current_series)
            next_days = _days_remaining_in_series(next_series)
            comp = compare_series(current_snap, next_snap, curr_days, next_days)
            if comp:
                advisor.append({
                    "symbol": sym,
                    "current_yield_pct": current_snap.annualised_yield_pct,
                    "current_bid_qty": current_snap.best_bid_qty,
                    "next_yield_pct": next_snap.annualised_yield_pct,
                    "next_bid_qty": next_snap.best_bid_qty,
                    "current_days": curr_days,
                    "next_days": next_days,
                    **comp,
                })

    return advisor
