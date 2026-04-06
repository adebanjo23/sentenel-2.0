"""Replay endpoints — historical threat replay and time-series analysis."""

from datetime import datetime, date

from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings, get_settings
from app.database import get_db
from app.services.replay_service import replay_snapshot, replay_timeline

router = APIRouter()


class ReplayRequest(BaseModel):
    cutoff_date: datetime
    state: str | None = None
    window_hours: int | None = None
    run_assessment: bool = False


@router.post("/")
async def historical_replay(
    request: ReplayRequest,
    db: AsyncSession = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    """Replay the threat map as it would have appeared on a historical date."""
    if request.cutoff_date > datetime.utcnow():
        raise HTTPException(status_code=400, detail="cutoff_date must be in the past")

    return await replay_snapshot(
        db=db,
        settings=settings,
        cutoff=request.cutoff_date,
        state=request.state,
        window_hours=request.window_hours,
        run_assessment=request.run_assessment,
    )


@router.get("/timeline")
async def threat_timeline(
    state: str = Query(..., description="Nigerian state name"),
    start_date: date = Query(..., description="Start date YYYY-MM-DD"),
    end_date: date = Query(..., description="End date YYYY-MM-DD"),
    window_hours: int | None = Query(None, description="Window size in hours"),
    db: AsyncSession = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    """Compute daily threat metrics for a state over a date range."""
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="end_date must be >= start_date")
    if (end_date - start_date).days > 90:
        raise HTTPException(status_code=400, detail="Date range cannot exceed 90 days")

    return await replay_timeline(
        db=db,
        settings=settings,
        state=state,
        start_date=datetime.combine(start_date, datetime.min.time()),
        end_date=datetime.combine(end_date, datetime.min.time()),
        window_hours=window_hours,
    )
