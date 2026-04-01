from fastapi import APIRouter, BackgroundTasks, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings, get_settings
from app.database import get_db
from app.services import acled_service

router = APIRouter()


@router.get("/status")
async def status(db: AsyncSession = Depends(get_db)):
    return await acled_service.get_stats(db)


@router.post("/sync")
async def sync(
    background_tasks: BackgroundTasks,
    since: str | None = Query(None, description="Start date YYYY-MM-DD"),
    historical: bool = Query(False, description="Fetch full historical data"),
    settings: Settings = Depends(get_settings),
):
    background_tasks.add_task(acled_service.sync, settings, since, historical)
    return {"status": "started", "historical": historical, "since": since}
