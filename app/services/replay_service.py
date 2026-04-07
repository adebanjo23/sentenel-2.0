"""Historical replay and time-series threat tracking."""

import logging
from collections import Counter
from datetime import datetime, timedelta
from types import SimpleNamespace

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.models import TwitterPost
from app.services.pipeline.stage_aggregate import (
    compute_state_metrics,
    should_flag_for_assessment,
)
from app.services.pipeline.stage_assess import assess_state
from app.services.pipeline.stage_strategic import evaluate_state_conditions, LEVEL_ORDER

logger = logging.getLogger("sentinel.replay")


def _compute_metrics_from_tweets(
    window_tweets: list, baseline_count: int, window_hours: int, baseline_days: int,
) -> dict:
    """Compute metrics from pre-fetched tweets. Mirrors compute_state_metrics logic
    but operates in-memory to avoid N*2 DB queries for timeline computation."""
    incident_count = len(window_tweets)
    window_days = window_hours / 24.0
    baseline_period_days = baseline_days - (window_hours / 24.0)

    incident_rate = incident_count / window_days if window_days > 0 else 0
    baseline_rate = baseline_count / baseline_period_days if baseline_period_days > 0 else 0
    acceleration = incident_rate / baseline_rate if baseline_rate > 0 else (incident_rate * 10 if incident_rate > 0 else 0)

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

    return {
        "incident_count": incident_count,
        "incident_rate": round(incident_rate, 2),
        "baseline_rate": round(baseline_rate, 2),
        "acceleration": round(acceleration, 2),
        "severity_distribution": dict(severity_dist),
        "category_mix": dict(category_mix),
        "lgas_affected": len(lga_counter),
        "repeat_lgas": [lga for lga, count in lga_counter.items() if count >= 2],
        "fatalities": fatalities,
        "has_warning": has_warning,
    }


def _estimate_threat_level(metrics: dict, threshold: float, min_incidents: int) -> str:
    """Estimate threat level from metrics without GPT-4."""
    flagged = should_flag_for_assessment(metrics, threshold, min_incidents)
    if not flagged:
        return "NORMAL"

    severe = metrics["severity_distribution"].get("critical", 0) + metrics["severity_distribution"].get("high", 0)

    if metrics["has_warning"] and metrics["fatalities"] >= 10:
        return "CRITICAL"
    if metrics["fatalities"] >= 20:
        return "CRITICAL"
    if metrics["has_warning"] or metrics["fatalities"] >= 5:
        return "HIGH"
    if metrics["acceleration"] >= threshold * 3 or severe >= 5:
        return "HIGH"
    if metrics["acceleration"] >= threshold or severe >= 3:
        return "ELEVATED"
    return "ELEVATED"


# ---------------------------------------------------------------------------
# Feature 1: Historical Replay
# ---------------------------------------------------------------------------

async def replay_snapshot(
    db: AsyncSession,
    settings: Settings,
    cutoff: datetime,
    state: str | None = None,
    window_hours: int | None = None,
    run_assessment: bool = False,
) -> dict:
    """Replay the threat picture as of a historical date."""
    wh = window_hours or settings.pipeline_aggregate_window_hours
    threshold = settings.pipeline_threat_escalation_threshold
    min_incidents = settings.pipeline_min_incidents_for_assessment

    # Determine which states to analyze
    if state:
        states_to_analyze = [state]
    else:
        result = await db.execute(
            select(TwitterPost.ai_state)
            .where(
                TwitterPost.ai_classified_at.isnot(None),
                TwitterPost.posted_at <= cutoff,
            )
            .distinct()
        )
        states_to_analyze = [r[0] for r in result.all() if r[0] and r[0] != "Unknown"]

    logger.info(f"Replay as of {cutoff.isoformat()}: analyzing {len(states_to_analyze)} states...")

    state_results = []

    for state_name in states_to_analyze:
        metrics = await compute_state_metrics(
            db, state_name, wh, settings.pipeline_baseline_window_days, cutoff=cutoff,
        )

        flagged = should_flag_for_assessment(metrics, threshold, min_incidents)
        threat_estimate = _estimate_threat_level(metrics, threshold, min_incidents)

        # Compute strategic conditions (90-day window)
        strategic = None
        if settings.openai_api_key:
            strategic = await evaluate_state_conditions(db, settings, state_name, cutoff=cutoff)

        # Combined level = max(tactical, strategic)
        tactical_order = LEVEL_ORDER.get(threat_estimate, 0)
        strategic_order = LEVEL_ORDER.get(strategic["level"], 0) if strategic else 0
        combined_order = max(tactical_order, strategic_order)
        combined_level = {v: k for k, v in LEVEL_ORDER.items()}[combined_order]

        entry = {
            "state": state_name,
            "threat_level": combined_level,
            "tactical_level": threat_estimate,
            "strategic_level": strategic["level"] if strategic else "NORMAL",
            "strategic_score": strategic["score"] if strategic else 0.0,
            "strategic_conditions": strategic["conditions"] if strategic else [],
            "strategic_assessment": strategic["overall_assessment"] if strategic else None,
            "risk_areas": strategic["risk_areas"] if strategic else [],
            "flagged": flagged or (strategic_order >= LEVEL_ORDER["ELEVATED"] if strategic else False),
            "metrics": metrics,
            "assessment": None,
        }

        # Run GPT-4 assessment if requested and state is flagged (by tactical or strategic)
        if run_assessment and entry["flagged"] and settings.openai_api_key:
            stl_proxy = SimpleNamespace(
                state=state_name,
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
                needs_assessment=True,
            )

            assessment = await assess_state(
                db, settings, state_name, stl_proxy, run_id=0,
                cutoff=cutoff, persist=False,
            )
            if assessment:
                entry["assessment"] = assessment
                entry["threat_level"] = assessment.get("threat_level", threat_estimate)

        state_results.append(entry)

    # Sort by threat level
    level_order = {"CRITICAL": 0, "HIGH": 1, "ELEVATED": 2, "NORMAL": 3}
    state_results.sort(key=lambda s: level_order.get(s["threat_level"], 4))

    if state and len(state_results) == 1:
        return {
            "replay_date": cutoff.isoformat(),
            "window_hours": wh,
            **state_results[0],
        }

    return {
        "replay_date": cutoff.isoformat(),
        "window_hours": wh,
        "states": state_results,
    }


# ---------------------------------------------------------------------------
# Feature 2: Time-Series Timeline
# ---------------------------------------------------------------------------

async def replay_timeline(
    db: AsyncSession,
    settings: Settings,
    state: str,
    start_date: datetime,
    end_date: datetime,
    window_hours: int | None = None,
) -> dict:
    """Compute daily threat metrics for a state over a date range. Single DB query, in-memory computation."""
    wh = window_hours or settings.pipeline_aggregate_window_hours
    baseline_days = settings.pipeline_baseline_window_days
    threshold = settings.pipeline_threat_escalation_threshold
    min_incidents = settings.pipeline_min_incidents_for_assessment

    # Fetch all tweets we could possibly need in one query
    earliest_needed = start_date - timedelta(days=baseline_days)
    latest_needed = end_date + timedelta(days=1)

    result = await db.execute(
        select(TwitterPost)
        .where(
            TwitterPost.ai_state == state,
            TwitterPost.ai_classified_at.isnot(None),
            TwitterPost.posted_at >= earliest_needed,
            TwitterPost.posted_at <= latest_needed,
        )
        .order_by(TwitterPost.posted_at.asc())
    )
    all_tweets = result.scalars().all()

    logger.info(f"Timeline {state}: {start_date.date()} → {end_date.date()}, {len(all_tweets)} tweets loaded")

    # Compute metrics for each day
    timeline = []
    current = start_date

    while current <= end_date:
        day_cutoff = current.replace(hour=23, minute=59, second=59)
        window_start = day_cutoff - timedelta(hours=wh)
        baseline_start = day_cutoff - timedelta(days=baseline_days)

        # Filter tweets in memory
        window_tweets = [
            t for t in all_tweets
            if t.posted_at and window_start <= t.posted_at <= day_cutoff
        ]
        baseline_count = sum(
            1 for t in all_tweets
            if t.posted_at and baseline_start <= t.posted_at < window_start
        )

        metrics = _compute_metrics_from_tweets(window_tweets, baseline_count, wh, baseline_days)
        threat_estimate = _estimate_threat_level(metrics, threshold, min_incidents)

        timeline.append({
            "date": current.strftime("%Y-%m-%d"),
            "threat_level_estimate": threat_estimate,
            **metrics,
        })

        current += timedelta(days=1)

    return {
        "state": state,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "window_hours": wh,
        "timeline": timeline,
    }
