"""Report/export routes — CSV downloads."""
import csv
import io
from datetime import date

import sqlalchemy as sa
from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from db.database import fetch_all
from db.models import transactions

router = APIRouter()


@router.get("/export/ledger")
def export_ledger_csv(symbol: str = "", start: str = "", end: str = ""):
    """Export transactions as CSV download."""
    query = sa.select(transactions).order_by(transactions.c.trade_date.desc())

    if symbol:
        query = query.where(transactions.c.symbol == symbol.upper())
    if start:
        query = query.where(transactions.c.trade_date >= start)
    if end:
        query = query.where(transactions.c.trade_date <= end)

    rows = fetch_all(query)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Date", "Symbol", "ISIN", "Type", "Series", "Qty",
        "Rate %", "Fee/Share", "Gross Income", "STT", "GST",
        "Stamp Duty", "Other Charges", "Brokerage Charged",
        "Brokerage Payable (8%)", "Refund Due", "Net Income",
        "Broker", "Contract Note Ref",
    ])

    for r in rows:
        writer.writerow([
            r.trade_date, r.symbol, r.isin, r.transaction_type,
            r.series, r.quantity, r.lending_fee_rate, r.lending_fee_per_share,
            r.gross_income, r.stt, r.gst, r.stamp_duty, r.other_charges,
            r.gross_brokerage_charged, r.brokerage_payable, r.brokerage_refund_due,
            r.net_income, r.broker, r.contract_note_ref,
        ])

    output.seek(0)
    filename = f"slb_ledger_{date.today().isoformat()}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
