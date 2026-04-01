"""Monitor endpoints — trigger watchlist monitoring cycles."""

from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends
from pydantic import BaseModel

from app.config import Settings, get_settings
from app.services import monitor_service

router = APIRouter()


class MonitorRequest(BaseModel):
    tier: Optional[str] = None             # "1", "2", "3", "4"
    accounts: Optional[list[str]] = None   # explicit @handles
    max_tweets_per_account: int = 20
    headless: Optional[bool] = None


@router.post("/run")
async def run_cycle(
    request: MonitorRequest,
    background_tasks: BackgroundTasks,
    settings: Settings = Depends(get_settings),
):
    """Run a monitoring cycle — scrape watchlist accounts and process with AI."""
    background_tasks.add_task(
        monitor_service.run_monitoring_cycle,
        settings,
        tier=request.tier,
        accounts=request.accounts,
        max_tweets_per_account=request.max_tweets_per_account,
        headless=request.headless,
    )
    return {
        "status": "started",
        "tier": request.tier,
        "accounts": request.accounts,
    }


@router.get("/watchlist")
async def get_watchlist(tier: str | None = None):
    """View configured watchlist accounts."""
    accounts = monitor_service.load_watchlist(tier)
    return {
        "total": len(accounts),
        "tier": tier,
        "accounts": [
            {"handle": a["handle"], "name": a.get("name", ""), "category": a.get("category", "")}
            for a in accounts
        ],
    }
