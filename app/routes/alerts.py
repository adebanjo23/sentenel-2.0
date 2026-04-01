"""Alerts endpoints — view and acknowledge threat alerts."""

from datetime import datetime

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, desc, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import ThreatAlert

router = APIRouter()


@router.get("/")
async def list_alerts(
    db: AsyncSession = Depends(get_db),
    state: str | None = Query(None),
    severity: str | None = Query(None),
    acknowledged: bool | None = Query(None),
    limit: int = Query(50, le=200),
):
    """List alerts with optional filters."""
    query = select(ThreatAlert)

    if state:
        query = query.where(ThreatAlert.state.ilike(state))
    if severity:
        query = query.where(ThreatAlert.severity == severity.upper())
    if acknowledged is not None:
        query = query.where(ThreatAlert.acknowledged == acknowledged)

    query = query.order_by(desc(ThreatAlert.created_at)).limit(limit)
    result = await db.execute(query)
    alerts = result.scalars().all()

    return [
        {
            "id": a.id,
            "state": a.state,
            "alert_type": a.alert_type,
            "severity": a.severity,
            "previous_level": a.previous_level,
            "new_level": a.new_level,
            "title": a.title,
            "summary": a.summary,
            "primary_threat_areas": a.primary_threat_areas,
            "recommended_actions": a.recommended_actions,
            "acknowledged": a.acknowledged,
            "created_at": a.created_at.isoformat() if a.created_at else None,
        }
        for a in alerts
    ]


@router.get("/{alert_id}")
async def alert_detail(alert_id: int, db: AsyncSession = Depends(get_db)):
    """Get a single alert with full detail."""
    result = await db.execute(select(ThreatAlert).where(ThreatAlert.id == alert_id))
    a = result.scalar_one_or_none()
    if not a:
        return {"error": "Alert not found"}

    return {
        "id": a.id,
        "state": a.state,
        "alert_type": a.alert_type,
        "severity": a.severity,
        "previous_level": a.previous_level,
        "new_level": a.new_level,
        "title": a.title,
        "summary": a.summary,
        "primary_threat_areas": a.primary_threat_areas,
        "recommended_actions": a.recommended_actions,
        "acknowledged": a.acknowledged,
        "acknowledged_at": a.acknowledged_at.isoformat() if a.acknowledged_at else None,
        "assessment_id": a.assessment_id,
        "pipeline_run_id": a.pipeline_run_id,
        "created_at": a.created_at.isoformat() if a.created_at else None,
    }


@router.post("/{alert_id}/acknowledge")
async def acknowledge_alert(alert_id: int, db: AsyncSession = Depends(get_db)):
    """Mark an alert as acknowledged."""
    result = await db.execute(select(ThreatAlert).where(ThreatAlert.id == alert_id))
    alert = result.scalar_one_or_none()
    if not alert:
        return {"error": "Alert not found"}

    await db.execute(
        update(ThreatAlert).where(ThreatAlert.id == alert_id).values(
            acknowledged=True,
            acknowledged_at=datetime.utcnow(),
        )
    )
    await db.commit()
    return {"status": "acknowledged", "alert_id": alert_id}
