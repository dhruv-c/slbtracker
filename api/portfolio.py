"""Portfolio management routes — CRUD for tracked stocks."""
import logging

import sqlalchemy as sa
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from db.database import execute, fetch_all, fetch_one
from db.models import portfolio

logger = logging.getLogger(__name__)
router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("/", response_class=HTMLResponse)
def portfolio_page(request: Request):
    """Portfolio management page."""
    stocks = fetch_all(
        sa.select(portfolio).order_by(portfolio.c.active.desc(), portfolio.c.symbol)
    )
    return templates.TemplateResponse("portfolio.html", {
        "request": request,
        "stocks": stocks,
    })


@router.post("/add")
def add_stock(symbol: str, client_name: str = "Family Office", quantity: int = 0):
    """Add a stock to the portfolio."""
    symbol = symbol.strip().upper()
    if not symbol:
        return JSONResponse({"error": "Symbol is required"}, status_code=400)

    existing = fetch_one(
        sa.select(portfolio).where(portfolio.c.symbol == symbol)
    )
    if existing:
        # Reactivate if deactivated
        execute(
            portfolio.update()
            .where(portfolio.c.symbol == symbol)
            .values(active=1, client_name=client_name, quantity=quantity)
        )
        return {"status": "ok", "action": "reactivated"}

    execute(
        portfolio.insert().values(
            symbol=symbol,
            client_name=client_name,
            quantity=quantity,
        )
    )
    return {"status": "ok", "action": "added"}


@router.post("/update")
def update_stock(symbol: str, client_name: str = None, quantity: int = None):
    """Update stock details."""
    updates = {}
    if client_name is not None:
        updates["client_name"] = client_name
    if quantity is not None:
        updates["quantity"] = quantity

    if not updates:
        return JSONResponse({"error": "Nothing to update"}, status_code=400)

    execute(
        portfolio.update()
        .where(portfolio.c.symbol == symbol.upper())
        .values(**updates)
    )
    return {"status": "ok"}


@router.post("/deactivate")
def deactivate_stock(symbol: str):
    """Deactivate a stock (soft delete)."""
    execute(
        portfolio.update()
        .where(portfolio.c.symbol == symbol.upper())
        .values(active=0)
    )
    return {"status": "ok"}


@router.post("/activate")
def activate_stock(symbol: str):
    """Reactivate a stock."""
    execute(
        portfolio.update()
        .where(portfolio.c.symbol == symbol.upper())
        .values(active=1)
    )
    return {"status": "ok"}


@router.delete("/delete")
def delete_stock(symbol: str):
    """Permanently delete a stock."""
    execute(portfolio.delete().where(portfolio.c.symbol == symbol.upper()))
    return {"status": "ok"}
