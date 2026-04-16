"""
SLBM Tracker — FastAPI app entrypoint with APScheduler.
"""
import logging
import os
from contextlib import asynccontextmanager

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from config.settings import settings
from db.database import init_db

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler(timezone="Asia/Kolkata")


def setup_scheduler():
    """Register all scheduled jobs."""
    from jobs.slb_poller import poll_slb_rates
    from jobs.alert_engine import send_morning_summary, send_eod_summary, check_rate_thresholds
    from jobs.eod_prices import store_eod_prices
    from jobs.email_parser import poll_emails

    interval = settings.polling_interval_seconds

    # SLB Poller — every 60s, Mon-Fri, 9:15-15:30 IST
    scheduler.add_job(
        poll_slb_rates,
        IntervalTrigger(seconds=interval),
        id="slb_poller",
        name="SLB Rate Poller",
        replace_existing=True,
        max_instances=1,
    )

    # Morning summary — 9:15 AM Mon-Fri
    scheduler.add_job(
        send_morning_summary,
        CronTrigger(hour=9, minute=15, day_of_week="mon-fri"),
        id="morning_summary",
        name="Morning Summary",
        replace_existing=True,
    )

    # EOD summary — 3:30 PM Mon-Fri
    scheduler.add_job(
        send_eod_summary,
        CronTrigger(hour=15, minute=30, day_of_week="mon-fri"),
        id="eod_summary",
        name="EOD Summary",
        replace_existing=True,
    )

    # Rate threshold check — every 5 min during market hours
    scheduler.add_job(
        check_rate_thresholds,
        CronTrigger(
            minute="*/5", hour="9-15", day_of_week="mon-fri"
        ),
        id="rate_thresholds",
        name="Rate Threshold Check",
        replace_existing=True,
    )

    # EOD Prices — 3:35 PM with retries at 4:00 and 4:30
    scheduler.add_job(
        store_eod_prices,
        CronTrigger(hour=15, minute=35, day_of_week="mon-fri"),
        id="eod_prices",
        name="EOD Prices (3:35 PM)",
        replace_existing=True,
    )
    scheduler.add_job(
        store_eod_prices,
        CronTrigger(hour=16, minute=0, day_of_week="mon-fri"),
        id="eod_prices_retry1",
        name="EOD Prices Retry (4:00 PM)",
        replace_existing=True,
    )
    scheduler.add_job(
        store_eod_prices,
        CronTrigger(hour=16, minute=30, day_of_week="mon-fri"),
        id="eod_prices_retry2",
        name="EOD Prices Retry (4:30 PM)",
        replace_existing=True,
    )

    # Email parser — every 15 min during business hours
    scheduler.add_job(
        poll_emails,
        CronTrigger(minute="*/15", hour="9-18", day_of_week="mon-fri"),
        id="email_parser",
        name="Email Parser",
        replace_existing=True,
    )

    logger.info("All scheduled jobs registered")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown lifecycle."""
    init_db()
    setup_scheduler()
    scheduler.start()
    logger.info("SLBM Tracker started — scheduler running")
    yield
    scheduler.shutdown(wait=False)
    logger.info("SLBM Tracker shutting down")


app = FastAPI(
    title="SLBM Tracker",
    description="NSE Securities Lending & Borrowing tracker for Family Office",
    lifespan=lifespan,
)

# Static files and templates
os.makedirs("static/css", exist_ok=True)
os.makedirs("static/js", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Include API routers
from api.dashboard import router as dashboard_router
from api.portfolio import router as portfolio_router
from api.reports import router as reports_router

app.include_router(dashboard_router)
app.include_router(portfolio_router, prefix="/portfolio", tags=["portfolio"])
app.include_router(reports_router, prefix="/reports", tags=["reports"])


@app.get("/health")
def health():
    return {
        "status": "ok",
        "scheduler_running": scheduler.running,
        "jobs": [j.name for j in scheduler.get_jobs()],
    }


@app.post("/poll-now")
def poll_now():
    """Manually trigger SLB rate poll."""
    from jobs.slb_poller import poll_slb_rates
    try:
        poll_slb_rates()
        return {"status": "ok", "message": "Poll completed"}
    except Exception as e:
        logger.error(f"Manual poll failed: {e}")
        return {"status": "error", "message": str(e)}
