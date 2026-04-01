from fastapi import APIRouter, BackgroundTasks, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings, get_settings
from app.database import get_db
from app.services import firms_service

router = APIRouter()


@router.get("/status")
async def status(db: AsyncSession = Depends(get_db)):
    return await firms_service.get_stats(db)


@router.post("/fetch")
async def fetch(
    background_tasks: BackgroundTasks,
    source: str | None = Query(None, description="Sensor source e.g. VIIRS_NOAA20_NRT"),
    days: int = Query(2, ge=1, description="Days of data to fetch (API limit: 5 per request, larger values are chunked automatically)"),
    all_sources: bool = Query(False, description="Fetch from all configured sources"),
    settings: Settings = Depends(get_settings),
):
    background_tasks.add_task(firms_service.sync, settings, source, days, all_sources)
    return {"status": "started", "source": source, "days": days, "all_sources": all_sources}
