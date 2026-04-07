"""Replay endpoints — historical threat replay and time-series analysis."""

import json
import hashlib
from datetime import datetime, date

from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings, get_settings
from app.database import get_db
from app.models import ReplayCache
from app.services.replay_service import replay_snapshot, replay_timeline

router = APIRouter()


def _cache_key(cutoff: datetime, state: str | None, window_hours: int | None, run_assessment: bool) -> str:
    raw = f"{cutoff.isoformat()}|{state or 'all'}|{window_hours or 'default'}|{run_assessment}"
    return hashlib.sha256(raw.encode()).hexdigest()


class ReplayRequest(BaseModel):
    cutoff_date: datetime
    state: str | None = None
    window_hours: int | None = None
    run_assessment: bool = False
    force_fresh: bool = False


@router.post("/")
async def historical_replay(
    request: ReplayRequest,
    db: AsyncSession = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    """Replay the threat map as it would have appeared on a historical date."""
    if request.cutoff_date > datetime.utcnow():
        raise HTTPException(status_code=400, detail="cutoff_date must be in the past")

    key = _cache_key(request.cutoff_date, request.state, request.window_hours, request.run_assessment)

    # Check cache unless force_fresh
    if not request.force_fresh:
        cached = await db.execute(
            select(ReplayCache).where(ReplayCache.cache_key == key)
        )
        hit = cached.scalar_one_or_none()
        if hit:
            result = hit.result_json
            result["cached"] = True
            result["cached_at"] = hit.created_at.isoformat() if hit.created_at else None
            return result

    # Run fresh analysis
    result = await replay_snapshot(
        db=db,
        settings=settings,
        cutoff=request.cutoff_date,
        state=request.state,
        window_hours=request.window_hours,
        run_assessment=request.run_assessment,
    )

    # Cache the result
    try:
        existing = await db.execute(
            select(ReplayCache).where(ReplayCache.cache_key == key)
        )
        old = existing.scalar_one_or_none()
        if old:
            old.result_json = result
            old.created_at = datetime.utcnow()
        else:
            cache_entry = ReplayCache(
                cache_key=key,
                state=request.state,
                cutoff_date=request.cutoff_date,
                window_hours=request.window_hours,
                result_json=result,
            )
            db.add(cache_entry)
        await db.commit()
    except Exception:
        pass  # Caching failure shouldn't break the response

    result["cached"] = False
    return result


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
