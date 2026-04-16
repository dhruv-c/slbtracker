import sqlalchemy as sa

metadata = sa.MetaData()

portfolio = sa.Table(
    "portfolio",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("symbol", sa.Text, nullable=False, unique=True),
    sa.Column("client_name", sa.Text, default="Family Office"),
    sa.Column("quantity", sa.Integer, default=0),
    sa.Column("added_at", sa.DateTime, server_default=sa.func.current_timestamp()),
    sa.Column("active", sa.Integer, default=1),
)

slb_snapshots = sa.Table(
    "slb_snapshots",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("snapshot_time", sa.DateTime, nullable=False),
    sa.Column("symbol", sa.Text, nullable=False),
    sa.Column("series", sa.Text, nullable=False),          # 'X5', 'X6'
    sa.Column("series_type", sa.Text, nullable=False),     # 'current', 'next'
    sa.Column("best_bid_qty", sa.Integer),
    sa.Column("best_bid_price", sa.Float),                 # lending fee per share (₹)
    sa.Column("best_offer_price", sa.Float),
    sa.Column("best_offer_qty", sa.Integer),
    sa.Column("ltp", sa.Float),                            # last traded lending fee
    sa.Column("underlying_ltp", sa.Float),                 # cash market stock price
    sa.Column("futures_ltp", sa.Float),
    sa.Column("spread", sa.Float),
    sa.Column("spread_pct", sa.Float),
    sa.Column("open_positions", sa.Integer),
    sa.Column("annualised_yield_pct", sa.Float),           # NSE pre-calculated
    sa.Column("volume", sa.Integer),
    sa.Column("turnover_inr", sa.Float),
    sa.Column("transaction_value_inr", sa.Float),
    sa.Column("ca_date", sa.Text),                         # corporate action date
    sa.Column("allow_recall", sa.Integer),                 # NNF: 0/1
    sa.Column("allow_repay", sa.Integer),                  # NNF: 0/1
    sa.UniqueConstraint("snapshot_time", "symbol", "series"),
)

daily_closes = sa.Table(
    "daily_closes",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("trade_date", sa.Date, nullable=False),
    sa.Column("symbol", sa.Text, nullable=False),
    sa.Column("close_price", sa.Float, nullable=False),
    sa.Column("prev_close", sa.Float),
    sa.Column("change_pct", sa.Float),
    sa.UniqueConstraint("trade_date", "symbol"),
)

transactions = sa.Table(
    "transactions",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("trade_date", sa.Date, nullable=False),
    sa.Column("symbol", sa.Text, nullable=False),
    sa.Column("isin", sa.Text),
    sa.Column("transaction_type", sa.Text, nullable=False),
    sa.Column("series", sa.Text, nullable=False),
    sa.Column("quantity", sa.Integer, nullable=False),
    sa.Column("lending_fee_rate", sa.Float),
    sa.Column("lending_fee_per_share", sa.Float),
    sa.Column("gross_income", sa.Float),
    sa.Column("stt", sa.Float),
    sa.Column("gst", sa.Float),
    sa.Column("stamp_duty", sa.Float),
    sa.Column("other_charges", sa.Float),
    sa.Column("net_income", sa.Float),
    sa.Column("gross_brokerage_charged", sa.Float),
    sa.Column("brokerage_payable", sa.Float),
    sa.Column("brokerage_refund_due", sa.Float),
    sa.Column("contract_note_ref", sa.Text),
    sa.Column("broker", sa.Text, default="HDFC"),
    sa.Column("parsed_at", sa.DateTime, server_default=sa.func.current_timestamp()),
    sa.Column("raw_pdf_text", sa.Text),
)

brokerage_refunds = sa.Table(
    "brokerage_refunds",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("quarter", sa.Text, nullable=False),
    sa.Column("total_charged", sa.Float),
    sa.Column("total_payable", sa.Float),
    sa.Column("total_refund_due", sa.Float),
    sa.Column("total_refund_received", sa.Float, default=0),
    sa.Column("status", sa.Text, default="PENDING"),
    sa.Column("notes", sa.Text),
    sa.Column("updated_at", sa.DateTime, server_default=sa.func.current_timestamp()),
)

alert_log = sa.Table(
    "alert_log",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("alert_time", sa.DateTime, server_default=sa.func.current_timestamp()),
    sa.Column("alert_type", sa.Text, nullable=False),
    sa.Column("symbol", sa.Text),
    sa.Column("series", sa.Text),
    sa.Column("message", sa.Text, nullable=False),
    sa.Column("telegram_sent", sa.Integer, default=0),
)

processed_emails = sa.Table(
    "processed_emails",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("gmail_message_id", sa.Text, nullable=False, unique=True),
    sa.Column("subject", sa.Text),
    sa.Column("processed_at", sa.DateTime, server_default=sa.func.current_timestamp()),
)

# Settings stored in DB (for Telegram config, thresholds, etc.)
app_settings = sa.Table(
    "app_settings",
    metadata,
    sa.Column("key", sa.Text, primary_key=True),
    sa.Column("value", sa.Text),
    sa.Column("updated_at", sa.DateTime, server_default=sa.func.current_timestamp()),
)

alert_thresholds = sa.Table(
    "alert_thresholds",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("symbol", sa.Text, nullable=False, unique=True),
    sa.Column("min_rate", sa.Float, default=0.0),
    sa.Column("updated_at", sa.DateTime, server_default=sa.func.current_timestamp()),
)
