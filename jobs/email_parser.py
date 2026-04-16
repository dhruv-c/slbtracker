"""
Email Parser — polls Gmail for broker contract notes, parses PDFs,
extracts transaction data, stores in ledger, sends Telegram confirmation.
"""
import base64
import io
import logging
import re
from datetime import datetime

import sqlalchemy as sa

from config.settings import settings
from db.database import execute, fetch_all, fetch_one
from db.models import processed_emails, transactions

logger = logging.getLogger(__name__)


def get_gmail_service():
    """Build Gmail API service using OAuth2 refresh token."""
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
    except ImportError:
        logger.error("Google API libraries not installed")
        return None

    client_id = settings.gmail_client_id
    client_secret = settings.gmail_client_secret
    refresh_token = settings.gmail_refresh_token

    if not all([client_id, client_secret, refresh_token]):
        logger.warning("Gmail credentials not configured")
        return None

    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        client_id=client_id,
        client_secret=client_secret,
        token_uri="https://oauth2.googleapis.com/token",
    )
    creds.refresh(Request())
    return build("gmail", "v1", credentials=creds)


def find_contract_note_emails(service) -> list[dict]:
    """Search Gmail for unprocessed contract note emails."""
    query = (
        'from:(hdfcsec OR icicisecurities OR icicidirect) '
        'subject:("contract note" OR "trade confirmation") '
        'has:attachment filename:pdf'
    )
    try:
        results = service.users().messages().list(
            userId="me", q=query, maxResults=20
        ).execute()
        messages = results.get("messages", [])
    except Exception as e:
        logger.error("Gmail search failed: %s", e)
        return []

    # Filter out already processed
    unprocessed = []
    for msg in messages:
        msg_id = msg["id"]
        existing = fetch_one(
            sa.select(processed_emails.c.id)
            .where(processed_emails.c.gmail_message_id == msg_id)
        )
        if not existing:
            unprocessed.append(msg)

    return unprocessed


def download_pdf_attachment(service, message_id: str) -> tuple[bytes, str] | None:
    """Download the first PDF attachment from a Gmail message."""
    try:
        msg = service.users().messages().get(
            userId="me", id=message_id, format="full"
        ).execute()

        parts = msg.get("payload", {}).get("parts", [])
        for part in parts:
            filename = part.get("filename", "")
            if filename.lower().endswith(".pdf"):
                att_id = part["body"].get("attachmentId")
                if att_id:
                    att = service.users().messages().attachments().get(
                        userId="me", messageId=message_id, id=att_id
                    ).execute()
                    data = base64.urlsafe_b64decode(att["data"])
                    return data, filename

        return None
    except Exception as e:
        logger.error("Failed to download attachment from %s: %s", message_id, e)
        return None


def parse_contract_note_pdf(pdf_bytes: bytes) -> list[dict]:
    """
    Extract SLB transaction data from a broker contract note PDF.
    Handles HDFC Securities and ICICI Securities formats.
    """
    try:
        import pdfplumber
    except ImportError:
        logger.error("pdfplumber not installed")
        return []

    parsed_transactions = []
    raw_text = ""

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                raw_text += text + "\n"
    except Exception as e:
        logger.error("PDF extraction failed: %s", e)
        return []

    # Detect broker
    broker = "HDFC"
    if "icici" in raw_text.lower():
        broker = "ICICI"

    # Extract trade date
    trade_date = None
    date_patterns = [
        r"Trade Date[:\s]*(\d{2}[/-]\d{2}[/-]\d{4})",
        r"Date[:\s]*(\d{2}[/-]\d{2}[/-]\d{4})",
        r"(\d{2}[/-]\d{2}[/-]\d{4})",
    ]
    for pat in date_patterns:
        m = re.search(pat, raw_text)
        if m:
            ds = m.group(1).replace("/", "-")
            try:
                trade_date = datetime.strptime(ds, "%d-%m-%Y").date()
            except ValueError:
                pass
            break

    # Extract contract note reference
    cn_ref = None
    cn_match = re.search(r"Contract Note No[.:\s]*([A-Z0-9/-]+)", raw_text, re.IGNORECASE)
    if cn_match:
        cn_ref = cn_match.group(1)

    # Parse SLB-specific lines
    # Pattern: SYMBOL ISIN SERIES QTY RATE AMOUNT ...
    slb_pattern = re.compile(
        r"([A-Z]+(?:\s?[A-Z]+)*)\s+"        # symbol
        r"(INE[A-Z0-9]+)\s+"                 # ISIN
        r"(\d{2})\s+"                         # series (01-12)
        r"(LEND|BORROW|SLB[- ]?LEND|SLB[- ]?BORROW)\s+"  # type
        r"(\d+)\s+"                           # quantity
        r"([\d.]+)\s+"                        # rate or fee
        r"([\d,.]+)",                         # amount
        re.IGNORECASE,
    )

    for m in slb_pattern.finditer(raw_text):
        symbol = m.group(1).strip().upper()
        isin = m.group(2).strip()
        series = m.group(3).strip()
        txn_type = "LEND" if "LEND" in m.group(4).upper() else "BORROW"
        quantity = int(m.group(5))
        rate = float(m.group(6))
        amount = float(m.group(7).replace(",", ""))

        # Extract charges from nearby text
        stt = _extract_charge(raw_text, "STT", amount)
        gst = _extract_charge(raw_text, "GST", amount)
        stamp_duty = _extract_charge(raw_text, "Stamp", amount)
        other_charges = _extract_charge(raw_text, "Other", amount)

        gross_income = amount
        gross_brokerage = _extract_charge(raw_text, "Brokerage", amount)
        if gross_brokerage is None:
            gross_brokerage = gross_income * 0.15  # default 15% assumption

        brokerage_payable = gross_income * settings.brokerage_deal_rate
        refund_due = gross_brokerage - brokerage_payable

        total_charges = sum(filter(None, [stt, gst, stamp_duty, other_charges, gross_brokerage]))
        net_income = gross_income - total_charges

        parsed_transactions.append({
            "trade_date": trade_date,
            "symbol": symbol,
            "isin": isin,
            "transaction_type": txn_type,
            "series": series,
            "quantity": quantity,
            "lending_fee_rate": rate,
            "lending_fee_per_share": amount / quantity if quantity else 0,
            "gross_income": gross_income,
            "stt": stt,
            "gst": gst,
            "stamp_duty": stamp_duty,
            "other_charges": other_charges,
            "net_income": net_income,
            "gross_brokerage_charged": gross_brokerage,
            "brokerage_payable": brokerage_payable,
            "brokerage_refund_due": refund_due,
            "contract_note_ref": cn_ref,
            "broker": broker,
            "raw_pdf_text": raw_text[:5000],  # first 5k chars for audit
        })

    # If regex didn't match, try table extraction as fallback
    if not parsed_transactions:
        logger.warning("Regex parsing found no SLB transactions, trying table fallback")
        parsed_transactions = _parse_tables_fallback(pdf_bytes, raw_text, trade_date, cn_ref, broker)

    return parsed_transactions


def _extract_charge(text: str, label: str, base_amount: float) -> float | None:
    """Try to extract a specific charge amount from contract note text."""
    patterns = [
        rf"{label}[:\s]*([\d,.]+)",
        rf"{label}.*?([\d,.]+)",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                val = float(m.group(1).replace(",", ""))
                if val < base_amount:  # sanity check
                    return val
            except ValueError:
                pass
    return None


def _parse_tables_fallback(pdf_bytes: bytes, raw_text: str, trade_date, cn_ref, broker) -> list[dict]:
    """Fallback: extract tables from PDF pages and parse row by row."""
    try:
        import pdfplumber
    except ImportError:
        return []

    results = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    if not table or len(table) < 2:
                        continue
                    headers = [str(h).strip().lower() if h else "" for h in table[0]]

                    # Look for SLB-related columns
                    has_slb = any("slb" in h or "lend" in h or "series" in h for h in headers)
                    if not has_slb:
                        continue

                    for row in table[1:]:
                        if not row or not any(row):
                            continue
                        row_dict = {headers[i]: (row[i] or "").strip() for i in range(min(len(headers), len(row)))}
                        # Try to build a transaction from table row
                        txn = _build_transaction_from_table_row(row_dict, trade_date, cn_ref, broker, raw_text)
                        if txn:
                            results.append(txn)
    except Exception as e:
        logger.error("Table fallback parsing failed: %s", e)

    return results


def _build_transaction_from_table_row(row: dict, trade_date, cn_ref, broker, raw_text) -> dict | None:
    """Try to extract a transaction from a table row dictionary."""
    symbol = None
    for key in ["symbol", "scrip", "security"]:
        if key in row and row[key]:
            symbol = row[key].upper()
            break
    if not symbol:
        return None

    quantity = 0
    for key in ["qty", "quantity"]:
        if key in row and row[key]:
            try:
                quantity = int(float(row[key].replace(",", "")))
            except ValueError:
                pass
            break

    if quantity == 0:
        return None

    rate = 0.0
    for key in ["rate", "fee rate", "lending rate"]:
        if key in row and row[key]:
            try:
                rate = float(row[key].replace(",", ""))
            except ValueError:
                pass
            break

    amount = 0.0
    for key in ["amount", "gross", "value"]:
        if key in row and row[key]:
            try:
                amount = float(row[key].replace(",", ""))
            except ValueError:
                pass
            break

    if amount == 0:
        return None

    gross_brokerage = amount * 0.15
    brokerage_payable = amount * settings.brokerage_deal_rate

    return {
        "trade_date": trade_date,
        "symbol": symbol,
        "isin": row.get("isin", ""),
        "transaction_type": "LEND",
        "series": row.get("series", ""),
        "quantity": quantity,
        "lending_fee_rate": rate,
        "lending_fee_per_share": amount / quantity if quantity else 0,
        "gross_income": amount,
        "stt": None,
        "gst": None,
        "stamp_duty": None,
        "other_charges": None,
        "net_income": amount - gross_brokerage,
        "gross_brokerage_charged": gross_brokerage,
        "brokerage_payable": brokerage_payable,
        "brokerage_refund_due": gross_brokerage - brokerage_payable,
        "contract_note_ref": cn_ref,
        "broker": broker,
        "raw_pdf_text": raw_text[:5000],
    }


def poll_emails():
    """Main function called by scheduler. Polls Gmail and processes contract notes."""
    logger.info("Polling Gmail for contract notes...")

    service = get_gmail_service()
    if not service:
        return

    emails = find_contract_note_emails(service)
    if not emails:
        logger.info("No new contract note emails found")
        return

    logger.info("Found %d unprocessed contract note emails", len(emails))

    for email_msg in emails:
        msg_id = email_msg["id"]
        try:
            result = download_pdf_attachment(service, msg_id)
            if not result:
                logger.warning("No PDF attachment in message %s", msg_id)
                _mark_processed(msg_id, "(no PDF)")
                continue

            pdf_bytes, filename = result
            txns = parse_contract_note_pdf(pdf_bytes)

            for txn in txns:
                execute(transactions.insert().values(**txn))

            _mark_processed(msg_id, filename)

            # Send Telegram confirmation
            if txns:
                from jobs.alert_engine import send_telegram, log_alert
                for txn in txns:
                    msg = (
                        f"✅ Contract note parsed: {txn['symbol']} {txn['transaction_type'].lower()}, "
                        f"₹{txn['net_income']:,.0f} net income, "
                        f"₹{txn['brokerage_refund_due']:,.0f} refund due"
                    )
                    send_telegram(msg)
                    log_alert("NEW_BID", msg, symbol=txn["symbol"])

            logger.info("Processed %d transactions from %s", len(txns), filename)

        except Exception as e:
            logger.error("Failed to process email %s: %s", msg_id, e)


def _mark_processed(gmail_id: str, subject: str = ""):
    """Mark a Gmail message as processed."""
    execute(
        processed_emails.insert().values(
            gmail_message_id=gmail_id,
            subject=subject,
        )
    )
