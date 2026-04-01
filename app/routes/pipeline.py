"""Pipeline endpoints — trigger and monitor the 5-stage intelligence pipeline."""

from fastapi import APIRouter, BackgroundTasks, Depends, Query
from pydantic import BaseModel
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings, get_settings
from app.database import get_db
from app.models import PipelineRun
from app.services.pipeline import run_pipeline

router = APIRouter()


class PipelineRequest(BaseModel):
    stages: list[int] | None = None  # e.g. [1, 2, 3] or None for all


@router.post("/run")
async def trigger_pipeline(
    request: PipelineRequest,
    background_tasks: BackgroundTasks,
    settings: Settings = Depends(get_settings),
):
    """Trigger the full intelligence pipeline (or specific stages)."""
    background_tasks.add_task(run_pipeline, settings, request.stages)
    return {"status": "started", "stages": request.stages or [1, 2, 3, 4, 5]}


@router.get("/status")
async def pipeline_status(
    db: AsyncSession = Depends(get_db),
    limit: int = Query(10, le=50),
):
    """List recent pipeline runs."""
    result = await db.execute(
        select(PipelineRun).order_by(desc(PipelineRun.id)).limit(limit)
    )
    runs = result.scalars().all()

    return [
        {
            "id": r.id,
            "status": r.status,
            "started_at": r.started_at.isoformat() if r.started_at else None,
            "completed_at": r.completed_at.isoformat() if r.completed_at else None,
            "stages": {
                "filter": {
                    "tweets_in": r.stage1_tweets_in,
                    "passed": r.stage1_tweets_passed,
                    "filtered": r.stage1_tweets_filtered,
                    "completed": r.stage1_completed_at is not None,
                },
                "classify": {
                    "classified": r.stage2_tweets_classified,
                    "events_created": r.stage2_events_created,
                    "events_updated": r.stage2_events_updated,
                    "completed": r.stage2_completed_at is not None,
                },
                "aggregate": {
                    "states_analyzed": r.stage3_states_analyzed,
                    "states_flagged": r.stage3_states_flagged,
                    "completed": r.stage3_completed_at is not None,
                },
                "assess": {
                    "assessments": r.stage4_assessments_created,
                    "completed": r.stage4_completed_at is not None,
                },
                "alert": {
                    "alerts": r.stage5_alerts_created,
                    "completed": r.stage5_completed_at is not None,
                },
            },
            "error": r.error_message,
        }
        for r in runs
    ]


@router.get("/runs/{run_id}")
async def pipeline_run_detail(run_id: int, db: AsyncSession = Depends(get_db)):
    """Get details of a specific pipeline run."""
    result = await db.execute(select(PipelineRun).where(PipelineRun.id == run_id))
    r = result.scalar_one_or_none()
    if not r:
        return {"error": "Pipeline run not found"}

    return {
        "id": r.id,
        "status": r.status,
        "started_at": r.started_at.isoformat() if r.started_at else None,
        "completed_at": r.completed_at.isoformat() if r.completed_at else None,
        "stages": {
            "filter": {
                "tweets_in": r.stage1_tweets_in,
                "passed": r.stage1_tweets_passed,
                "filtered": r.stage1_tweets_filtered,
                "completed_at": r.stage1_completed_at.isoformat() if r.stage1_completed_at else None,
            },
            "classify": {
                "classified": r.stage2_tweets_classified,
                "events_created": r.stage2_events_created,
                "events_updated": r.stage2_events_updated,
                "completed_at": r.stage2_completed_at.isoformat() if r.stage2_completed_at else None,
            },
            "aggregate": {
                "states_analyzed": r.stage3_states_analyzed,
                "states_flagged": r.stage3_states_flagged,
                "completed_at": r.stage3_completed_at.isoformat() if r.stage3_completed_at else None,
            },
            "assess": {
                "assessments": r.stage4_assessments_created,
                "completed_at": r.stage4_completed_at.isoformat() if r.stage4_completed_at else None,
            },
            "alert": {
                "alerts": r.stage5_alerts_created,
                "completed_at": r.stage5_completed_at.isoformat() if r.stage5_completed_at else None,
            },
        },
        "metadata": r.run_metadata,
        "error": r.error_message,
    }
