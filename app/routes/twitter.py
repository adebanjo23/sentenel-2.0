from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings, get_settings
from app.database import get_db
from app.services import twitter_service

router = APIRouter()


class SearchRequest(BaseModel):
    query: str
    count: Optional[int] = None
    headless: Optional[bool] = None  # Override: False = show browser for login


class UserScrapeRequest(BaseModel):
    username: str
    count: Optional[int] = None
    headless: Optional[bool] = None


@router.get("/status")
async def status(db: AsyncSession = Depends(get_db)):
    return await twitter_service.get_stats(db)


@router.post("/search")
async def search(
    request: SearchRequest,
    background_tasks: BackgroundTasks,
    settings: Settings = Depends(get_settings),
):
    background_tasks.add_task(
        twitter_service.search_tweets, settings, request.query, request.count, request.headless
    )
    return {"status": "started", "query": request.query}


@router.post("/scrape-user")
async def scrape_user(
    request: UserScrapeRequest,
    background_tasks: BackgroundTasks,
    settings: Settings = Depends(get_settings),
):
    background_tasks.add_task(
        twitter_service.scrape_user, settings, request.username, request.count, request.headless
    )
    return {"status": "started", "username": request.username}
