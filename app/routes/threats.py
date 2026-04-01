"""Threats endpoints — view state-level threat assessments."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import StateThreatLevel, ThreatAssessment

router = APIRouter()


@router.get("/")
async def list_threats(db: AsyncSession = Depends(get_db)):
    """List all state threat levels, sorted by severity."""
    level_order = {"CRITICAL": 0, "HIGH": 1, "ELEVATED": 2, "NORMAL": 3}

    result = await db.execute(select(StateThreatLevel))
    states = result.scalars().all()

    sorted_states = sorted(states, key=lambda s: level_order.get(s.threat_level, 4))

    return [
        {
            "state": s.state,
            "threat_level": s.threat_level,
            "incident_count": s.incident_count_window,
            "incident_rate": s.incident_rate,
            "baseline_rate": s.baseline_rate,
            "acceleration": s.acceleration,
            "severity_distribution": s.severity_distribution,
            "category_mix": s.category_mix,
            "lgas_affected": s.lgas_affected,
            "repeat_lgas": s.repeat_lgas,
            "fatalities": s.fatalities_window,
            "last_assessment_at": s.last_assessment_at.isoformat() if s.last_assessment_at else None,
            "updated_at": s.updated_at.isoformat() if s.updated_at else None,
        }
        for s in sorted_states
    ]


@router.get("/{state}")
async def state_threat(
    state: str,
    db: AsyncSession = Depends(get_db),
):
    """Get current threat level and recent assessments for a state."""
    # Find state (case-insensitive)
    result = await db.execute(
        select(StateThreatLevel).where(StateThreatLevel.state.ilike(state))
    )
    stl = result.scalar_one_or_none()

    if not stl:
        return {"error": f"No data for state: {state}"}

    # Get recent assessments
    assessments_result = await db.execute(
        select(ThreatAssessment)
        .where(ThreatAssessment.state == stl.state)
        .order_by(desc(ThreatAssessment.created_at))
        .limit(5)
    )
    assessments = assessments_result.scalars().all()

    return {
        "state": stl.state,
        "threat_level": stl.threat_level,
        "metrics": {
            "incident_count": stl.incident_count_window,
            "incident_rate": stl.incident_rate,
            "baseline_rate": stl.baseline_rate,
            "acceleration": stl.acceleration,
            "severity_distribution": stl.severity_distribution,
            "category_mix": stl.category_mix,
            "lgas_affected": stl.lgas_affected,
            "repeat_lgas": stl.repeat_lgas,
            "fatalities": stl.fatalities_window,
        },
        "assessments": [
            {
                "id": a.id,
                "threat_level": a.threat_level,
                "previous_level": a.previous_threat_level,
                "primary_threat_areas": a.primary_threat_areas,
                "threat_timeframe": a.threat_timeframe,
                "key_indicators": a.key_indicators,
                "specific_warnings": a.specific_warnings,
                "recommended_actions": a.recommended_actions,
                "narrative_summary": a.narrative_summary,
                "tweets_analyzed": a.tweets_analyzed,
                "created_at": a.created_at.isoformat() if a.created_at else None,
            }
            for a in assessments
        ],
    }


@router.get("/{state}/history")
async def state_threat_history(
    state: str,
    db: AsyncSession = Depends(get_db),
    limit: int = Query(20, le=100),
):
    """Historical threat assessments for a state."""
    result = await db.execute(
        select(ThreatAssessment)
        .where(ThreatAssessment.state.ilike(state))
        .order_by(desc(ThreatAssessment.created_at))
        .limit(limit)
    )
    assessments = result.scalars().all()

    return [
        {
            "id": a.id,
            "threat_level": a.threat_level,
            "previous_level": a.previous_threat_level,
            "primary_threat_areas": a.primary_threat_areas,
            "threat_timeframe": a.threat_timeframe,
            "key_indicators": a.key_indicators,
            "specific_warnings": a.specific_warnings,
            "recommended_actions": a.recommended_actions,
            "narrative_summary": a.narrative_summary,
            "incident_count": a.incident_count,
            "tweets_analyzed": a.tweets_analyzed,
            "created_at": a.created_at.isoformat() if a.created_at else None,
        }
        for a in assessments
    ]
