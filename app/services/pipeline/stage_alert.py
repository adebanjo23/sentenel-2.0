"""Stage 5: Alert — generate alerts when state threat levels change."""

import logging
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.database import get_session
from app.models import StateThreatLevel, ThreatAssessment, ThreatAlert

logger = logging.getLogger("sentinel.pipeline.alert")

LEVEL_ORDER = {"NORMAL": 0, "ELEVATED": 1, "HIGH": 2, "CRITICAL": 3}


def determine_alert_type(previous: str, new: str) -> str | None:
    """Determine alert type based on level transition."""
    prev_order = LEVEL_ORDER.get(previous, 0)
    new_order = LEVEL_ORDER.get(new, 0)

    if new_order <= prev_order:
        return None  # No alert for downward or same-level transitions

    if new == "CRITICAL":
        return "new_critical"
    return "escalation"


async def run_alert(settings: Settings, run_id: int) -> dict:
    """Stage 5: Generate alerts for states where threat level has increased."""
    db = await get_session()
    try:
        # Get all states that just had an assessment in this pipeline run
        result = await db.execute(
            select(ThreatAssessment)
            .where(ThreatAssessment.pipeline_run_id == run_id)
        )
        assessments = result.scalars().all()

        if not assessments:
            logger.info("Stage 5: No assessments to check for alerts")
            return {"alerts": 0}

        alerts_created = 0

        for assessment in assessments:
            previous = assessment.previous_threat_level or "NORMAL"
            new = assessment.threat_level

            alert_type = determine_alert_type(previous, new)
            if not alert_type:
                continue

            # Build alert title
            title = f"{assessment.state} threat level escalated: {previous} → {new}"

            # Build summary from assessment
            summary_parts = []
            if assessment.narrative_summary:
                summary_parts.append(assessment.narrative_summary[:500])
            if assessment.specific_warnings:
                warnings = assessment.specific_warnings
                if isinstance(warnings, list) and warnings:
                    summary_parts.append("Warnings: " + "; ".join(warnings[:3]))

            alert = ThreatAlert(
                assessment_id=assessment.id,
                pipeline_run_id=run_id,
                state=assessment.state,
                alert_type=alert_type,
                severity=new,
                previous_level=previous,
                new_level=new,
                title=title,
                summary="\n\n".join(summary_parts) if summary_parts else None,
                primary_threat_areas=assessment.primary_threat_areas,
                recommended_actions=assessment.recommended_actions,
            )
            db.add(alert)
            alerts_created += 1
            logger.info(f"  ALERT: {title}")

        await db.commit()
        logger.info(f"Stage 5 complete: {alerts_created} alerts created")
        return {"alerts": alerts_created}
    finally:
        await db.close()
