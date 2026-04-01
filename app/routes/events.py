"""Events endpoints — view intelligence events produced by the agent."""

from fastapi import APIRouter, BackgroundTasks, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings, get_settings
from app.database import get_db
from app.services import intel_agent

router = APIRouter()


@router.get("/")
async def list_events(
    db: AsyncSession = Depends(get_db),
    status: str = Query("active"),
    severity: str | None = Query(None),
    state: str | None = Query(None, description="Nigerian state e.g. Borno"),
    limit: int = Query(50, le=200),
):
    """List intelligence events with optional filters."""
    return await intel_agent.get_events(db, status=status, severity=severity, admin1=state, limit=limit)


@router.get("/{event_id}")
async def event_detail(event_id: int, db: AsyncSession = Depends(get_db)):
    """Get a single event with linked sources (tweets, FIRMS hotspots)."""
    event = await intel_agent.get_event_detail(db, event_id)
    if not event:
        return {"error": "Event not found"}
    return event


@router.post("/process")
async def process_tweets(
    background_tasks: BackgroundTasks,
    settings: Settings = Depends(get_settings),
):
    """Manually trigger intelligence processing on unanalyzed tweets."""
    background_tasks.add_task(intel_agent.process_new_tweets, settings)
    return {"status": "started"}
