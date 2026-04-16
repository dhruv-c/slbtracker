"""
Alert Engine — evaluates conditions and sends Telegram messages.
Handles: morning summary, new bid alerts, rate threshold alerts, EOD summary,
and lending advisor comparisons.
"""
import calendar
import logging
from datetime import datetime, date, timedelta

import requests
import sqlalchemy as sa

from config.settings import settings, Settings
from db.database import execute, fetch_all, fetch_one, get_setting
from db.models import (
    alert_log,
    alert_thresholds,
    brokerage_refunds,
    portfolio,
    slb_snapshots,
    transactions,
)

logger = logging.getLogger(__name__)


# ── Telegram ──────────────────────────────────────────────────────────

def send_telegram(message: str) -> bool:
    """Send a message via Telegram Bot API."""
    token = get_setting("telegram_bot_token", settings.telegram_bot_token)
    chat_id = get_setting("telegram_chat_id", settings.telegram_chat_id)

    if not token or not chat_id:
        logger.warning("Telegram not configured, skipping alert")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=15,
        )
        if resp.status_code == 200:
            logger.info("Telegram message sent")
            return True
        else:
            logger.error("Telegram send failed: %s %s", resp.status_code, resp.text)
            return False
    except Exception as e:
        logger.error("Telegram send error: %s", e)
        return False


def log_alert(alert_type: str, message: str, symbol: str = None,
              series: str = None, sent: bool = False):
    execute(
        alert_log.insert().values(
            alert_type=alert_type, symbol=symbol, series=series,
            message=message, telegram_sent=1 if sent else 0,
        )
    )


# ── Lending Advisor ───────────────────────────────────────────────────

def _days_remaining_in_series(series: str) -> int:
    """Days remaining until series expiry (last Thursday of that month)."""
    today = date.today()
    month = Settings.series_to_month(series)
    if month == 0:
        return 30
    year = today.year
    if month < today.month:
        year += 1

    cal = calendar.monthcalendar(year, month)
    last_thursday = None
    for week in reversed(cal):
        if week[3] != 0:
            last_thursday = week[3]
            break
    if last_thursday is None:
        last_thursday = 28

    expiry = date(year, month, last_thursday)
    return max((expiry - today).days, 1)


def compare_series(current_snap, next_snap, current_days, next_days) -> dict | None:
    """
    Compare total yield of current vs next month series.
    Uses annualised_yield_pct directly from NSE CSV.
    Accepts either dict (from poller) or Row (from DB).
    """
    def _g(obj, key):
        if obj is None:
            return None
        return obj.get(key) if isinstance(obj, dict) else getattr(obj, key, None)

    curr_yield_ann = _g(current_snap, "annualised_yield_pct") or 0
    next_yield_ann = _g(next_snap, "annualised_yield_pct") or 0

    if not curr_yield_ann and not next_yield_ann:
        return None

    stt = settings.stt_rate
    curr_abs = (curr_yield_ann / 100) * (current_days / 365)
    next_abs = (next_yield_ann / 100) * (next_days / 365)
    curr_net = curr_abs - stt
    next_net = next_abs - stt

    curr_spread = _g(current_snap, "spread_pct") or 0
    next_spread = _g(next_snap, "spread_pct") or 0

    recommendation = "current" if curr_net >= next_net else "next"
    diff_bps = abs(curr_net - next_net) * 10000

    return {
        "recommendation": recommendation,
        "current_annual_yield_pct": curr_yield_ann,
        "next_annual_yield_pct": next_yield_ann,
        "current_net_yield_pct": round(curr_net * 100, 4),
        "next_net_yield_pct": round(next_net * 100, 4),
        "difference_bps": round(diff_bps, 1),
        "current_spread_pct": curr_spread,
        "next_spread_pct": next_spread,
        "current_days_remaining": current_days,
        "next_days_remaining": next_days,
    }


# ── Helpers ───────────────────────────────────────────────────────────

def _latest_snap(symbol: str, series: str):
    return fetch_one(
        sa.select(slb_snapshots)
        .where(slb_snapshots.c.symbol == symbol)
        .where(slb_snapshots.c.series == series)
        .order_by(slb_snapshots.c.snapshot_time.desc())
        .limit(1)
    )


def _latest_snap_with_bid(symbol: str, series: str):
    return fetch_one(
        sa.select(slb_snapshots)
        .where(slb_snapshots.c.symbol == symbol)
        .where(slb_snapshots.c.series == series)
        .where(slb_snapshots.c.best_bid_qty.isnot(None))
        .where(slb_snapshots.c.best_bid_qty > 0)
        .order_by(slb_snapshots.c.snapshot_time.desc())
        .limit(1)
    )


def _fmt(val, prefix="", suffix="", decimals=2) -> str:
    if val is None:
        return "-"
    if isinstance(val, int) or (isinstance(val, float) and val == int(val) and abs(val) > 100):
        return f"{prefix}{val:,.0f}{suffix}"
    return f"{prefix}{val:,.{decimals}f}{suffix}"


def _g(obj, key):
    """Get a key from a dict or attribute from a Row."""
    if obj is None:
        return None
    return obj.get(key) if isinstance(obj, dict) else getattr(obj, key, None)


# ── Alert Functions ───────────────────────────────────────────────────

def send_new_bid_alerts(new_bids: list[dict]):
    """Send Telegram alert for each new bid — side-by-side series comparison."""
    current_series, next_series = settings.get_active_series()
    curr_days = _days_remaining_in_series(current_series)
    next_days = _days_remaining_in_series(next_series)

    for bid in new_bids:
        symbol = bid["symbol"]
        series = bid["series"]
        other_series = next_series if series == current_series else current_series
        other = _latest_snap(symbol, other_series)

        # Arrange current / next
        if series == current_series:
            curr, nxt = bid, other
        else:
            curr, nxt = other, bid

        msg = f"🔔 <b>New SLB Bid — {symbol}</b>\n\n"
        msg += f"<b>Series {current_series} (current) | Series {next_series} (next)</b>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += (
            f"Bid:        {_fmt(_g(curr,'best_bid_price'),'₹')} × {_fmt(_g(curr,'best_bid_qty'))} qty"
            f"    | {_fmt(_g(nxt,'best_bid_price'),'₹')} × {_fmt(_g(nxt,'best_bid_qty'))} qty\n"
        )
        msg += (
            f"Offer:      {_fmt(_g(curr,'best_offer_price'),'₹')} × {_fmt(_g(curr,'best_offer_qty'))} qty"
            f"    | {_fmt(_g(nxt,'best_offer_price'),'₹')} × {_fmt(_g(nxt,'best_offer_qty'))} qty\n"
        )
        msg += f"LTP:        {_fmt(_g(curr,'ltp'),'₹')}             | {_fmt(_g(nxt,'ltp'),'₹')}\n"
        msg += (
            f"Yield:      {_fmt(_g(curr,'annualised_yield_pct'),suffix='% p.a.')}"
            f"        | {_fmt(_g(nxt,'annualised_yield_pct'),suffix='% p.a.')}\n"
        )
        msg += (
            f"Spread:     {_fmt(_g(curr,'spread'),'₹')} ({_fmt(_g(curr,'spread_pct'),suffix='%')})"
            f"     | {_fmt(_g(nxt,'spread'),'₹')} ({_fmt(_g(nxt,'spread_pct'),suffix='%')})\n"
        )
        msg += (
            f"Open Int:   {_fmt(_g(curr,'open_positions'))}"
            f"         | {_fmt(_g(nxt,'open_positions'))}\n"
        )
        msg += (
            f"Underlying: {_fmt(_g(curr,'underlying_ltp'),'₹')}"
            f"         | {_fmt(_g(nxt,'underlying_ltp'),'₹')}\n"
        )

        # Lending advisor comparison
        curr_yield = _g(curr, "annualised_yield_pct") or 0
        nxt_yield = _g(nxt, "annualised_yield_pct") or 0
        if curr_yield > 0 or nxt_yield > 0:
            stt = settings.stt_rate
            c_net = (curr_yield / 100) * (curr_days / 365) - stt
            n_net = (nxt_yield / 100) * (next_days / 365) - stt
            rec = "current" if c_net >= n_net else "next"
            rec_series = current_series if rec == "current" else next_series
            diff = abs(c_net - n_net) * 10000
            msg += (
                f"\n📊 <b>Lending advisor:</b>\n"
                f"• {current_series}: {curr_yield:.2f}% × {curr_days}d = {c_net*100:.2f}% net of STT\n"
                f"• {next_series}: {nxt_yield:.2f}% × {next_days}d = {n_net*100:.2f}% net of STT\n"
                f"→ <b>Lend in {rec_series} ({rec} month) — {diff:.0f} bps better</b>\n"
            )

        # NNF flags
        if _g(bid, "allow_recall") == 0:
            msg += "\n⚠️ Early recall NOT permitted for this contract\n"

        # Rate threshold
        threshold_row = fetch_one(
            sa.select(alert_thresholds.c.min_rate)
            .where(alert_thresholds.c.symbol == symbol)
        )
        yield_val = _g(bid, "annualised_yield_pct") or 0
        if threshold_row and yield_val >= threshold_row.min_rate:
            msg += f"\n⚠️ Yield {yield_val:.2f}% crossed threshold {threshold_row.min_rate:.2f}%\n"

        sent = send_telegram(msg)
        log_alert("NEW_BID", msg, symbol=symbol, series=series, sent=sent)


def send_morning_summary():
    """9:15 AM summary of active bids for portfolio stocks."""
    current_series, next_series = settings.get_active_series()
    active_bids = []
    no_bids = []

    symbols = fetch_all(sa.select(portfolio.c.symbol).where(portfolio.c.active == 1))
    for row in symbols:
        sym = row.symbol
        has_bid = False
        for series in [current_series, next_series]:
            snap = _latest_snap_with_bid(sym, series)
            if snap:
                active_bids.append({
                    "symbol": sym, "series": series,
                    "yield_pct": snap.annualised_yield_pct,
                    "qty": snap.best_bid_qty,
                })
                has_bid = True
        if not has_bid:
            no_bids.append(sym)

    msg = f"📊 <b>SLB Morning Summary — {date.today().strftime('%d %b %Y')}</b>\n\n"
    if active_bids:
        msg += "Active bids for your portfolio:\n"
        for b in active_bids:
            y = f"{b['yield_pct']:.2f}% p.a." if b["yield_pct"] else "N/A"
            msg += f"🟢 {b['symbol']} — Series {b['series']}: {y} | Qty: {b['qty'] or 0}\n"
    else:
        msg += "No active bids for your portfolio stocks.\n"
    if no_bids:
        msg += f"\nNo bids: {', '.join(no_bids)}\n"

    sent = send_telegram(msg)
    log_alert("MORNING_SUMMARY", msg, sent=sent)


def send_eod_summary():
    """3:30 PM end-of-day summary."""
    today = date.today()
    today_start = datetime.combine(today, datetime.min.time())

    traded = fetch_all(
        sa.select(
            slb_snapshots.c.symbol,
            slb_snapshots.c.series,
            sa.func.max(slb_snapshots.c.underlying_ltp).label("close"),
        )
        .where(slb_snapshots.c.snapshot_time >= today_start)
        .where(slb_snapshots.c.volume.isnot(None))
        .where(slb_snapshots.c.volume > 0)
        .group_by(slb_snapshots.c.symbol, slb_snapshots.c.series)
    )

    open_count_row = fetch_one(
        sa.select(sa.func.count()).select_from(transactions)
        .where(transactions.c.transaction_type == "LEND")
    )
    open_count = open_count_row[0] if open_count_row else 0

    pending = fetch_one(
        sa.select(
            sa.func.coalesce(sa.func.sum(brokerage_refunds.c.total_refund_due), 0)
            - sa.func.coalesce(sa.func.sum(brokerage_refunds.c.total_refund_received), 0)
        ).where(brokerage_refunds.c.status != "SETTLED")
    )
    pending_refund = pending[0] if pending else 0

    msg = f"📈 <b>SLB EOD Summary — {today.strftime('%d %b %Y')}</b>\n\n"
    if traded:
        msg += "Traded today:\n"
        for t in traded:
            close_str = f"₹{t.close:,.2f}" if t.close else "N/A"
            msg += f"• {t.symbol} Series {t.series}: {close_str} close\n"
    else:
        msg += "No SLB trades observed today.\n"

    msg += f"\nOpen lending positions: {open_count}\n"
    if pending_refund > 0:
        msg += f"Brokerage refund pending: ₹{pending_refund:,.0f}\n"

    sent = send_telegram(msg)
    log_alert("EOD_SUMMARY", msg, sent=sent)


def check_rate_thresholds():
    """Check if any annualised yields have crossed user-defined thresholds."""
    thresholds = fetch_all(sa.select(alert_thresholds))
    if not thresholds:
        return

    current_series, next_series = settings.get_active_series()

    for t in thresholds:
        for series in [current_series, next_series]:
            snap = fetch_one(
                sa.select(slb_snapshots)
                .where(slb_snapshots.c.symbol == t.symbol)
                .where(slb_snapshots.c.series == series)
                .where(slb_snapshots.c.annualised_yield_pct.isnot(None))
                .order_by(slb_snapshots.c.snapshot_time.desc())
                .limit(1)
            )
            if not snap or not snap.annualised_yield_pct:
                continue
            if snap.annualised_yield_pct < t.min_rate:
                continue

            one_hour_ago = datetime.now() - timedelta(hours=1)
            recent = fetch_one(
                sa.select(alert_log)
                .where(alert_log.c.alert_type == "RATE_THRESHOLD")
                .where(alert_log.c.symbol == t.symbol)
                .where(alert_log.c.series == series)
                .where(alert_log.c.alert_time >= one_hour_ago)
            )
            if recent:
                continue

            msg = (
                f"⚠️ <b>Rate Alert — {t.symbol}</b>\n\n"
                f"Yield {snap.annualised_yield_pct:.2f}% has crossed your threshold of {t.min_rate:.2f}%\n"
                f"Series: {series} | Bid Qty: {snap.best_bid_qty or 0}\n"
            )
            sent = send_telegram(msg)
            log_alert("RATE_THRESHOLD", msg, symbol=t.symbol, series=series, sent=sent)
