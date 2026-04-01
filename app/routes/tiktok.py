from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings, get_settings
from app.database import get_db
from app.services import tiktok_service

router = APIRouter()


class ScrapeRequest(BaseModel):
    keywords: Optional[list[str]] = None
    hashtags: Optional[list[str]] = None
    users: Optional[list[str]] = None
    max_per_keyword: Optional[int] = None
    max_per_hashtag: Optional[int] = None
    max_per_user: Optional[int] = None


@router.get("/status")
async def status(db: AsyncSession = Depends(get_db)):
    return await tiktok_service.get_stats(db)


@router.post("/scrape")
async def scrape(
    background_tasks: BackgroundTasks,
    request: ScrapeRequest = ScrapeRequest(),
    settings: Settings = Depends(get_settings),
):
    background_tasks.add_task(
        tiktok_service.run_scrape,
        settings,
        keywords=request.keywords,
        hashtags=request.hashtags,
        users=request.users,
        max_per_keyword=request.max_per_keyword,
        max_per_hashtag=request.max_per_hashtag,
        max_per_user=request.max_per_user,
    )
    return {"status": "started"}
