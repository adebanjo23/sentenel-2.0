"""Stage 3: Aggregate — compute state-level metrics, detect spikes."""

import logging
from collections import Counter, defaultdict
from datetime import datetime, timedelta

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.database import get_session
from app.models import TwitterPost, StateThreatLevel

logger = logging.getLogger("sentinel.pipeline.aggregate")

NIGERIAN_STATES = [
    "Abia", "Adamawa", "Akwa Ibom", "Anambra", "Bauchi", "Bayelsa", "Benue",
    "Borno", "Cross River", "Delta", "Ebonyi", "Edo", "Ekiti", "Enugu",
    "Gombe", "Imo", "Jigawa", "Kaduna", "Kano", "Katsina", "Kebbi",
    "Kogi", "Kwara", "Lagos", "Nasarawa", "Niger", "Ogun", "Ondo",
    "Osun", "Oyo", "Plateau", "Rivers", "Sokoto", "Taraba", "Yobe", "Zamfara",
    "FCT",
]


async def compute_state_metrics(
    db: AsyncSession, state: str, window_hours: int, baseline_days: int,
    cutoff: datetime | None = None,
) -> dict:
    """Compute security metrics for a single state. Uses cutoff instead of now if provided."""
    now = cutoff or datetime.utcnow()
    window_start = now - timedelta(hours=window_hours)
    baseline_start = now - timedelta(days=baseline_days)

    # Current window: classified tweets for this state
    query = (
        select(TwitterPost)
        .where(
            TwitterPost.ai_state == state,
            TwitterPost.ai_classified_at.isnot(None),
            TwitterPost.posted_at >= window_start,
            TwitterPost.posted_at <= now,
        )
    )
    result = await db.execute(query)
    window_tweets = result.scalars().all()

    # Baseline: classified tweets for this state in the longer period (excluding current window)
    baseline_result = await db.execute(
        select(func.count(TwitterPost.id))
        .where(
            TwitterPost.ai_state == state,
            TwitterPost.ai_classified_at.isnot(None),
            TwitterPost.posted_at >= baseline_start,
            TwitterPost.posted_at < window_start,
        )
    )
    baseline_count = baseline_result.scalar() or 0

    incident_count = len(window_tweets)
    window_days = window_hours / 24.0
    baseline_period_days = baseline_days - (window_hours / 24.0)

    incident_rate = incident_count / window_days if window_days > 0 else 0
    baseline_rate = baseline_count / baseline_period_days if baseline_period_days > 0 else 0
    acceleration = incident_rate / baseline_rate if baseline_rate > 0 else (incident_rate * 10 if incident_rate > 0 else 0)

    # Severity distribution
    severity_dist = Counter()
    category_mix = Counter()
    lga_counter = Counter()
    fatalities = 0
    has_warning = False

    for t in window_tweets:
        if t.ai_severity:
            severity_dist[t.ai_severity] += 1
        if t.ai_category:
            category_mix[t.ai_category] += 1
            if t.ai_category == "warning":
                has_warning = True
        if t.ai_lga:
            lga_counter[t.ai_lga] += 1
        if t.ai_fatalities_mentioned:
            fatalities += t.ai_fatalities_mentioned

    lgas_affected = len(lga_counter)
    repeat_lgas = [lga for lga, count in lga_counter.items() if count >= 2]

    return {
        "incident_count": incident_count,
        "incident_rate": round(incident_rate, 2),
        "baseline_rate": round(baseline_rate, 2),
        "acceleration": round(acceleration, 2),
        "severity_distribution": dict(severity_dist),
        "category_mix": dict(category_mix),
        "lgas_affected": lgas_affected,
        "repeat_lgas": repeat_lgas,
        "fatalities": fatalities,
        "has_warning": has_warning,
    }


def should_flag_for_assessment(metrics: dict, threshold: float, min_incidents: int) -> bool:
    """Determine if a state needs a threat assessment."""
    if metrics["incident_count"] < min_incidents:
        return False

    # Explicit warning signal always triggers assessment
    if metrics["has_warning"]:
        return True

    # Acceleration above threshold
    if metrics["acceleration"] >= threshold:
        return True

    # High severity incidents
    severe = metrics["severity_distribution"].get("critical", 0) + metrics["severity_distribution"].get("high", 0)
    if severe >= 3:
        return True

    # Many LGAs affected (geographic spread)
    if metrics["lgas_affected"] >= 4:
        return True

    # Fatalities
    if metrics["fatalities"] >= 5:
        return True

    return False


async def run_aggregate(settings: Settings, run_id: int) -> dict:
    """Stage 3: Compute metrics for all states and flag those needing assessment."""
    db = await get_session()
    try:
        states_analyzed = 0
        states_flagged = 0

        # Find which states have classified tweets
        result = await db.execute(
            select(TwitterPost.ai_state)
            .where(TwitterPost.ai_classified_at.isnot(None))
            .distinct()
        )
        active_states = [r[0] for r in result.all() if r[0] and r[0] != "Unknown"]

        logger.info(f"Stage 3: Analyzing {len(active_states)} states with data...")

        for state in active_states:
            metrics = await compute_state_metrics(
                db, state,
                settings.pipeline_aggregate_window_hours,
                settings.pipeline_baseline_window_days,
            )

            needs_assessment = should_flag_for_assessment(
                metrics,
                settings.pipeline_threat_escalation_threshold,
                settings.pipeline_min_incidents_for_assessment,
            )

            # Upsert StateThreatLevel
            existing = await db.execute(
                select(StateThreatLevel).where(StateThreatLevel.state == state)
            )
            stl = existing.scalar_one_or_none()

            if stl:
                stl.incident_count_window = metrics["incident_count"]
                stl.incident_rate = metrics["incident_rate"]
                stl.baseline_rate = metrics["baseline_rate"]
                stl.acceleration = metrics["acceleration"]
                stl.severity_distribution = metrics["severity_distribution"]
                stl.category_mix = metrics["category_mix"]
                stl.lgas_affected = metrics["lgas_affected"]
                stl.repeat_lgas = metrics["repeat_lgas"]
                stl.fatalities_window = metrics["fatalities"]
                stl.needs_assessment = needs_assessment
                stl.updated_at = datetime.utcnow()
            else:
                stl = StateThreatLevel(
                    state=state,
                    threat_level="NORMAL",
                    incident_count_window=metrics["incident_count"],
                    incident_rate=metrics["incident_rate"],
                    baseline_rate=metrics["baseline_rate"],
                    acceleration=metrics["acceleration"],
                    severity_distribution=metrics["severity_distribution"],
                    category_mix=metrics["category_mix"],
                    lgas_affected=metrics["lgas_affected"],
                    repeat_lgas=metrics["repeat_lgas"],
                    fatalities_window=metrics["fatalities"],
                    needs_assessment=needs_assessment,
                    updated_at=datetime.utcnow(),
                )
                db.add(stl)

            states_analyzed += 1
            if needs_assessment:
                states_flagged += 1
                logger.info(
                    f"  {state}: FLAGGED — {metrics['incident_count']} incidents, "
                    f"{metrics['acceleration']}x acceleration, warning={metrics['has_warning']}"
                )

            await db.commit()

        logger.info(f"Stage 3 complete: {states_flagged}/{states_analyzed} states flagged")
        return {"states_analyzed": states_analyzed, "states_flagged": states_flagged}
    finally:
        await db.close()
