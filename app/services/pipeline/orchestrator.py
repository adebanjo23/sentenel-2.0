"""Pipeline orchestrator — sequences the 5 intelligence processing stages."""

import logging
from datetime import datetime

from sqlalchemy import update

from app.config import Settings
from app.database import get_session
from app.models import PipelineRun

from app.services.pipeline.stage_filter import run_filter
from app.services.pipeline.stage_classify import run_classify
from app.services.pipeline.stage_aggregate import run_aggregate
from app.services.pipeline.stage_assess import run_assess
from app.services.pipeline.stage_alert import run_alert

logger = logging.getLogger("sentinel.pipeline")


async def run_pipeline(settings: Settings, stages: list[int] | None = None) -> int:
    """
    Run the 5-stage intelligence pipeline.

    Args:
        settings: Application settings
        stages: Optional list of stage numbers to run (e.g., [1, 2]).
                 None means run all stages 1-5.

    Returns:
        PipelineRun ID
    """
    db = await get_session()
    stages_to_run = stages or [1, 2, 3, 4, 5]
    run_id = 0

    try:
        run = PipelineRun(
            status="running",
            run_metadata={"stages": stages_to_run},
        )
        db.add(run)
        await db.commit()
        await db.refresh(run)
        run_id = run.id
        logger.info(f"Pipeline run #{run_id} started, stages: {stages_to_run}")
    except Exception as e:
        logger.error(f"Failed to create pipeline run: {e}")
        await db.close()
        return 0

    await db.close()

    try:
        # Stage 1: Filter
        if 1 in stages_to_run:
            try:
                s1 = await run_filter(settings, run_id)
                db = await get_session()
                await db.execute(
                    update(PipelineRun).where(PipelineRun.id == run_id).values(
                        stage1_tweets_in=s1["tweets_in"],
                        stage1_tweets_passed=s1["passed"],
                        stage1_tweets_filtered=s1["filtered"],
                        stage1_completed_at=datetime.utcnow(),
                    )
                )
                await db.commit()
                await db.close()
                logger.info(f"Stage 1: {s1['passed']}/{s1['tweets_in']} passed filter")
            except Exception as e:
                logger.error(f"Stage 1 failed: {e}")

        # Stage 2: Classify
        if 2 in stages_to_run:
            try:
                s2 = await run_classify(settings, run_id)
                db = await get_session()
                await db.execute(
                    update(PipelineRun).where(PipelineRun.id == run_id).values(
                        stage2_tweets_classified=s2["classified"],
                        stage2_events_created=s2["events_created"],
                        stage2_events_updated=s2["events_updated"],
                        stage2_completed_at=datetime.utcnow(),
                    )
                )
                await db.commit()
                await db.close()
                logger.info(f"Stage 2: {s2['classified']} classified, {s2['events_created']} events created")
            except Exception as e:
                logger.error(f"Stage 2 failed: {e}")

        # Stage 3: Aggregate
        if 3 in stages_to_run:
            try:
                s3 = await run_aggregate(settings, run_id)
                db = await get_session()
                await db.execute(
                    update(PipelineRun).where(PipelineRun.id == run_id).values(
                        stage3_states_analyzed=s3["states_analyzed"],
                        stage3_states_flagged=s3["states_flagged"],
                        stage3_completed_at=datetime.utcnow(),
                    )
                )
                await db.commit()
                await db.close()
                logger.info(f"Stage 3: {s3['states_flagged']}/{s3['states_analyzed']} states flagged")
            except Exception as e:
                logger.error(f"Stage 3 failed: {e}")

        # Stage 4: Assess
        if 4 in stages_to_run:
            try:
                s4 = await run_assess(settings, run_id)
                db = await get_session()
                await db.execute(
                    update(PipelineRun).where(PipelineRun.id == run_id).values(
                        stage4_assessments_created=s4["assessments"],
                        stage4_completed_at=datetime.utcnow(),
                    )
                )
                await db.commit()
                await db.close()
                logger.info(f"Stage 4: {s4['assessments']} assessments")
            except Exception as e:
                logger.error(f"Stage 4 failed: {e}")

        # Stage 5: Alert
        if 5 in stages_to_run:
            try:
                s5 = await run_alert(settings, run_id)
                db = await get_session()
                await db.execute(
                    update(PipelineRun).where(PipelineRun.id == run_id).values(
                        stage5_alerts_created=s5["alerts"],
                        stage5_completed_at=datetime.utcnow(),
                    )
                )
                await db.commit()
                await db.close()
                logger.info(f"Stage 5: {s5['alerts']} alerts")
            except Exception as e:
                logger.error(f"Stage 5 failed: {e}")

        # Mark complete
        db = await get_session()
        await db.execute(
            update(PipelineRun).where(PipelineRun.id == run_id).values(
                status="completed",
                completed_at=datetime.utcnow(),
            )
        )
        await db.commit()
        await db.close()
        logger.info(f"Pipeline run #{run_id} completed")

    except Exception as e:
        logger.error(f"Pipeline run #{run_id} failed: {e}")
        try:
            db = await get_session()
            await db.execute(
                update(PipelineRun).where(PipelineRun.id == run_id).values(
                    status="failed",
                    completed_at=datetime.utcnow(),
                    error_message=str(e),
                )
            )
            await db.commit()
            await db.close()
        except Exception:
            pass

    return run_id
