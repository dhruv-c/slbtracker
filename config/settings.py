from datetime import date
from typing import ClassVar
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    # Telegram
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    # Gmail OAuth2
    gmail_client_id: str = ""
    gmail_client_secret: str = ""
    gmail_refresh_token: str = ""
    gmail_email_address: str = ""

    # App
    secret_key: str = "change-me-in-production"
    database_path: str = "data/slbm.db"
    polling_interval_seconds: int = 60
    log_level: str = "INFO"

    # Series overrides (blank = auto-calculate)
    current_series: str = ""
    next_series: str = ""

    # Brokerage
    brokerage_deal_rate: float = 0.08
    stt_rate: float = 0.0015

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    # X-series month mapping: Jan=X1 ... Sep=X9, Oct=XA, Nov=XB, Dec=XC
    X_SERIES_MAP: ClassVar[dict[int, str]] = {
        1: "X1", 2: "X2", 3: "X3", 4: "X4", 5: "X5", 6: "X6",
        7: "X7", 8: "X8", 9: "X9", 10: "XA", 11: "XB", 12: "XC",
    }

    def get_active_series(self) -> tuple[str, str]:
        if self.current_series and self.next_series:
            return self.current_series, self.next_series
        today = date.today()
        current = self.X_SERIES_MAP[today.month]
        next_month = today.month % 12 + 1
        nxt = self.X_SERIES_MAP[next_month]
        return current, nxt

    MONTH_NAMES: ClassVar[dict[int, str]] = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
        7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
    }

    @staticmethod
    def series_to_month(series: str) -> int:
        """Convert X-series code back to month number (e.g. 'X5' → 5, 'XA' → 10)."""
        reverse = {v: k for k, v in Settings.X_SERIES_MAP.items()}
        return reverse.get(series.upper(), 0)

    @staticmethod
    def series_label(series: str) -> str:
        """Return display label like 'X4 (Apr)' for use in UI."""
        month_num = Settings.series_to_month(series)
        if month_num:
            return f"{series} ({Settings.MONTH_NAMES[month_num]})"
        return series


settings = Settings()
