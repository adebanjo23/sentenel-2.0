"""Scheduler endpoints — control the automatic monitoring loop."""

from fastapi import APIRouter, Depends

from app.config import Settings, get_settings
from app.services.scheduler import scheduler

router = APIRouter()


@router.get("/status")
async def scheduler_status():
    """Get current scheduler status."""
    return scheduler.status()


@router.post("/start")
async def start_scheduler(settings: Settings = Depends(get_settings)):
    """Start the automatic monitoring cycle."""
    if scheduler.running:
        return {"status": "already_running", **scheduler.status()}
    scheduler.start(settings)
    return {"status": "started", **scheduler.status()}


@router.post("/stop")
async def stop_scheduler():
    """Stop the automatic monitoring cycle."""
    scheduler.stop()
    return {"status": "stopped"}


@router.post("/pause")
async def pause_scheduler():
    """Pause — skip cycles but keep the loop alive."""
    scheduler.pause()
    return {"status": "paused"}


@router.post("/resume")
async def resume_scheduler():
    """Resume after pause."""
    scheduler.resume()
    return {"status": "resumed"}
