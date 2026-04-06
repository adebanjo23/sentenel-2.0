"""Stage 4: Assess — produce AI threat assessments for flagged states."""

import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta

from openai import AsyncOpenAI
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.database import get_session
from app.models import TwitterPost, StateThreatLevel, ThreatAssessment, Event

logger = logging.getLogger("sentinel.pipeline.assess")

ASSESSMENT_PROMPT = """You are a senior Nigerian security intelligence analyst producing a threat assessment for {state} State.

Below are {tweet_count} security-related tweets from the last {window_hours} hours, organized by category.

{categorized_tweets}

METRICS:
- Incidents in window: {incident_count}
- Incident rate: {incident_rate}/day (baseline: {baseline_rate}/day, {acceleration}x acceleration)
- LGAs affected: {lgas_affected}
- Repeat LGAs: {repeat_lgas}
- Fatalities reported: {fatalities}

Produce a threat assessment in valid JSON:
{{
    "threat_level": "CRITICAL or HIGH or ELEVATED or NORMAL",
    "primary_threat_areas": ["LGA1", "LGA2"],
    "threat_timeframe": "description of when the threat is most acute",
    "key_indicators": ["indicator 1", "indicator 2", ...],
    "specific_warnings": ["any explicit warnings found in the data"],
    "recommended_actions": ["action 1", "action 2", ...],
    "narrative_summary": "2-3 paragraph assessment narrative"
}}

Threat level guidance:
- CRITICAL: Imminent or ongoing major attack, explicit warnings, extreme acceleration, mass casualties likely
- HIGH: Significant escalation, multiple active incidents, armed groups operating freely
- ELEVATED: Above-normal activity, concerning patterns, potential for escalation
- NORMAL: Routine security activity, no concerning patterns"""


async def assess_state(
    db: AsyncSession, settings: Settings, state: str, stl, run_id: int,
    cutoff: datetime | None = None, persist: bool = True,
) -> ThreatAssessment | dict | None:
    """Produce a threat assessment for one state. When persist=False, returns dict without saving."""
    now = cutoff or datetime.utcnow()
    window_start = now - timedelta(hours=settings.pipeline_aggregate_window_hours)

    # Get classified tweets for this state
    query = (
        select(TwitterPost)
        .where(
            TwitterPost.ai_state == state,
            TwitterPost.ai_classified_at.isnot(None),
            TwitterPost.posted_at >= window_start,
            TwitterPost.posted_at <= now,
        )
        .order_by(TwitterPost.posted_at.asc())
    )
    result = await db.execute(query)
    tweets = result.scalars().all()

    if not tweets:
        return None

    # Organize by category
    by_category = defaultdict(list)
    for t in tweets:
        cat = t.ai_category or "report"
        by_category[cat].append(t)

    # Format for the prompt
    categorized_text = ""
    for cat, cat_tweets in sorted(by_category.items()):
        categorized_text += f"\n{cat.upper()} ({len(cat_tweets)}):\n"
        for t in cat_tweets:
            date_str = t.posted_at.strftime("%b %d %H:%M") if t.posted_at else "?"
            categorized_text += f"- [{date_str}] @{t.author_handle or '?'}: {(t.content or '')[:250]}\n"

    repeat_lgas = stl.repeat_lgas if stl.repeat_lgas else []
    repeat_lgas_str = ", ".join(repeat_lgas) if repeat_lgas else "None"

    prompt = ASSESSMENT_PROMPT.format(
        state=state,
        tweet_count=len(tweets),
        window_hours=settings.pipeline_aggregate_window_hours,
        categorized_tweets=categorized_text,
        incident_count=stl.incident_count_window,
        incident_rate=stl.incident_rate,
        baseline_rate=stl.baseline_rate,
        acceleration=stl.acceleration,
        lgas_affected=stl.lgas_affected,
        repeat_lgas=repeat_lgas_str,
        fatalities=stl.fatalities_window,
    )

    # Temporal context for replay mode
    system_message = "You are a security intelligence analyst. Respond with valid JSON only. No markdown, no code blocks."
    if cutoff is not None:
        cutoff_str = cutoff.strftime("%B %d, %Y at %H:%M UTC")
        system_message = (
            f"You are a security intelligence analyst producing a threat assessment "
            f"as of {cutoff_str}. Only consider information available up to this date. "
            f"Respond with valid JSON only. No markdown, no code blocks."
        )

    client = AsyncOpenAI(api_key=settings.openai_api_key)

    try:
        response = await client.chat.completions.create(
            model=settings.pipeline_model_full,
            messages=[
                {"role": "system", "content": system_message},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        raw = response.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
            if raw.endswith("```"):
                raw = raw[:-3].strip()

        result = json.loads(raw)

        if not persist:
            # Replay mode — return dict without saving to DB
            logger.info(f"  [REPLAY] {state}: {result.get('threat_level', '?')} — {(result.get('narrative_summary') or '')[:100]}...")
            return {
                "state": state,
                "threat_level": result.get("threat_level", "ELEVATED"),
                "primary_threat_areas": result.get("primary_threat_areas"),
                "threat_timeframe": result.get("threat_timeframe"),
                "key_indicators": result.get("key_indicators"),
                "specific_warnings": result.get("specific_warnings"),
                "recommended_actions": result.get("recommended_actions"),
                "narrative_summary": result.get("narrative_summary"),
                "incident_count": stl.incident_count_window,
                "tweets_analyzed": len(tweets),
            }

        # Live mode — persist to DB
        event_result = await db.execute(
            select(Event.id)
            .where(Event.admin1 == state, Event.status == "active")
            .order_by(Event.last_updated.desc())
            .limit(20)
        )
        event_ids = [r[0] for r in event_result.all()]

        assessment = ThreatAssessment(
            pipeline_run_id=run_id,
            state=state,
            threat_level=result.get("threat_level", "ELEVATED"),
            previous_threat_level=stl.threat_level,
            primary_threat_areas=result.get("primary_threat_areas"),
            threat_timeframe=result.get("threat_timeframe"),
            key_indicators=result.get("key_indicators"),
            specific_warnings=result.get("specific_warnings"),
            recommended_actions=result.get("recommended_actions"),
            narrative_summary=result.get("narrative_summary"),
            incident_count=stl.incident_count_window,
            tweets_analyzed=len(tweets),
            events_referenced=event_ids,
        )
        db.add(assessment)
        await db.commit()
        await db.refresh(assessment)

        stl.threat_level = result.get("threat_level", stl.threat_level)
        stl.needs_assessment = False
        stl.last_assessment_id = assessment.id
        stl.last_assessment_at = datetime.utcnow()
        stl.updated_at = datetime.utcnow()
        await db.commit()

        logger.info(f"  {state}: {assessment.threat_level} — {assessment.narrative_summary[:100] if assessment.narrative_summary else ''}...")
        return assessment

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse assessment JSON for {state}: {e}")
        return None
    except Exception as e:
        logger.error(f"Assessment failed for {state}: {e}")
        return None


async def run_assess(settings: Settings, run_id: int) -> dict:
    """Stage 4: Generate threat assessments for all flagged states."""
    if not settings.openai_api_key:
        logger.warning("Stage 4: No OpenAI API key — skipping assessments")
        return {"assessments": 0}

    db = await get_session()
    try:
        result = await db.execute(
            select(StateThreatLevel).where(StateThreatLevel.needs_assessment == True)
        )
        flagged = result.scalars().all()

        if not flagged:
            logger.info("Stage 4: No states need assessment")
            return {"assessments": 0}

        logger.info(f"Stage 4: Assessing {len(flagged)} flagged states...")
        assessments_created = 0

        for stl in flagged:
            assessment = await assess_state(db, settings, stl.state, stl, run_id)
            if assessment:
                assessments_created += 1

        logger.info(f"Stage 4 complete: {assessments_created} assessments created")
        return {"assessments": assessments_created}
    finally:
        await db.close()
