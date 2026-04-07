"""Stage 3B: Strategic — evaluate 90-day threat conditions per state using AI."""

import json
import logging
from datetime import datetime, timedelta

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.database import get_session
from app.models import TwitterPost, StateThreatLevel

logger = logging.getLogger("sentinel.pipeline.strategic")

LEVEL_ORDER = {"NORMAL": 0, "ELEVATED": 1, "HIGH": 2, "CRITICAL": 3}

CONDITIONS_PROMPT = """You are a Nigerian security analyst evaluating long-term threat conditions for {state} State as of {cutoff_date}.

Below are {count} security-related tweets from the last 90 days about this state.

{tweets}

Evaluate which of these 9 threat conditions are CURRENTLY PRESENT based on the evidence above.
A condition is present if there is credible evidence it exists, even if the most recent tweet about it is weeks old.
For example, if the military bombed terrorist camps 2 weeks ago, the armed group is still a threat — they don't disappear after 72 hours.

CONDITIONS TO EVALUATE:
1. ARMED_GROUP_ACTIVE — Organized armed group confirmed operating in this state (evidence: military operations against camps/enclaves, airstrikes, "fighters neutralized", identified group names)
2. CIVILIAN_ATTACK — Attack on civilians with casualties reported in this state
3. RELIGIOUS_TARGET — Attack on or threat to religious institution (church, mosque, worship center)
4. INSTITUTIONS_CLOSED — Schools or public institutions closed due to insecurity
5. GOVERNMENT_INTERVENTION — Federal/state government deployed additional troops, declared emergency, or acknowledged elevated threat
6. ARMS_TRAFFICKING — Weapons, ammunition, or arms dealers intercepted in this state
7. COMMUNITY_WARNING — Community leaders or organizations raised alarm about specific threats or planned attacks
8. DISPLACEMENT — People fleeing, displaced, or communities deserted due to violence
9. REPEATED_ATTACKS — Multiple attacks in the same LGA/area showing a pattern

For each condition that IS present, provide:
- "condition": the condition name (e.g. "ARMED_GROUP_ACTIVE")
- "present": true
- "evidence": brief description of the evidence (1-2 sentences)
- "most_recent_date": date of the most recent evidence (YYYY-MM-DD)
- "severity": 1 (low concern), 2 (moderate), or 3 (high concern)

For conditions NOT present, omit them from the list.

Also provide:
- "overall_assessment": 1-2 sentences on whether this state faces an elevated long-term threat
- "risk_areas": list of specific LGAs or areas most at risk

Respond with valid JSON only:
{{"conditions": [...], "overall_assessment": "...", "risk_areas": [...]}}"""


CONDITION_WEIGHTS = {
    "ARMED_GROUP_ACTIVE": 2.0,
    "CIVILIAN_ATTACK": 2.0,
    "RELIGIOUS_TARGET": 1.5,
    "INSTITUTIONS_CLOSED": 1.0,
    "GOVERNMENT_INTERVENTION": 1.5,
    "ARMS_TRAFFICKING": 1.5,
    "COMMUNITY_WARNING": 2.5,
    "DISPLACEMENT": 1.0,
    "REPEATED_ATTACKS": 1.5,
}

CONDITION_DECAY_DAYS = {
    "ARMED_GROUP_ACTIVE": 60,
    "CIVILIAN_ATTACK": 45,
    "RELIGIOUS_TARGET": 60,
    "INSTITUTIONS_CLOSED": 45,
    "GOVERNMENT_INTERVENTION": 30,
    "ARMS_TRAFFICKING": 30,
    "COMMUNITY_WARNING": 14,
    "DISPLACEMENT": 30,
    "REPEATED_ATTACKS": 45,
}


def compute_strategic_score(conditions: list[dict], cutoff: datetime) -> tuple[float, str]:
    """Compute strategic threat score from AI-detected conditions."""
    total_score = 0.0

    for cond in conditions:
        name = cond.get("condition", "")
        if not cond.get("present", False):
            continue

        weight = CONDITION_WEIGHTS.get(name, 1.0)
        decay_days = CONDITION_DECAY_DAYS.get(name, 30)

        # Calculate recency
        most_recent = cond.get("most_recent_date")
        if most_recent:
            try:
                event_date = datetime.strptime(most_recent, "%Y-%m-%d")
                days_since = (cutoff - event_date).days
                recency = max(0.0, 1.0 - (days_since / decay_days))
            except (ValueError, TypeError):
                recency = 0.5  # Default if date parsing fails
        else:
            recency = 0.5

        # Severity multiplier (1-3 scale → 0.8-1.2 multiplier)
        severity = cond.get("severity", 2)
        severity_mult = 0.6 + (severity * 0.2)

        condition_score = weight * recency * severity_mult
        cond["score"] = round(condition_score, 2)
        total_score += condition_score

    total_score = round(total_score, 2)

    if total_score >= 6.0:
        level = "CRITICAL"
    elif total_score >= 4.0:
        level = "HIGH"
    elif total_score >= 2.0:
        level = "ELEVATED"
    else:
        level = "NORMAL"

    return total_score, level


async def evaluate_state_conditions(
    db: AsyncSession, settings: Settings, state: str, cutoff: datetime | None = None,
) -> dict:
    """Evaluate strategic threat conditions for one state using AI."""
    now = cutoff or datetime.utcnow()
    window_start = now - timedelta(days=settings.pipeline_strategic_window_days)

    # Fetch classified tweets for this state in the 90-day window
    result = await db.execute(
        select(TwitterPost)
        .where(
            TwitterPost.ai_state == state,
            TwitterPost.ai_classified_at.isnot(None),
            TwitterPost.posted_at >= window_start,
            TwitterPost.posted_at <= now,
        )
        .order_by(TwitterPost.posted_at.asc())
    )
    tweets = result.scalars().all()

    if len(tweets) < settings.pipeline_strategic_min_tweets:
        return {
            "state": state,
            "score": 0.0,
            "level": "NORMAL",
            "conditions": [],
            "overall_assessment": "Insufficient data for strategic assessment",
            "risk_areas": [],
            "tweets_analyzed": len(tweets),
        }

    # Format tweets for the prompt
    tweet_text = ""
    for t in tweets:
        date_str = t.posted_at.strftime("%Y-%m-%d") if t.posted_at else "?"
        cat = t.ai_category or "report"
        tweet_text += f"[{date_str}] ({cat}) @{t.author_handle or '?'}: {(t.content or '')[:250]}\n"

    cutoff_str = now.strftime("%B %d, %Y")
    prompt = CONDITIONS_PROMPT.format(
        state=state,
        cutoff_date=cutoff_str,
        count=len(tweets),
        tweets=tweet_text,
    )

    from app.utils.llm_client import llm_chat

    api_key = (
        settings.anthropic_api_key
        if settings.pipeline_strategic_provider == "anthropic"
        else settings.openai_api_key
    )

    try:
        raw = await llm_chat(
            provider=settings.pipeline_strategic_provider,
            model=settings.pipeline_strategic_model,
            system_message="You are a security analyst. Respond with valid JSON only. No markdown, no code blocks.",
            user_message=prompt,
            api_key=api_key,
        )

        result = json.loads(raw)
        conditions = result.get("conditions", [])
        score, level = compute_strategic_score(conditions, now)

        return {
            "state": state,
            "score": score,
            "level": level,
            "conditions": conditions,
            "overall_assessment": result.get("overall_assessment", ""),
            "risk_areas": result.get("risk_areas", []),
            "tweets_analyzed": len(tweets),
        }

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse strategic conditions JSON for {state}: {e}")
        return {"state": state, "score": 0.0, "level": "NORMAL", "conditions": [], "overall_assessment": "", "risk_areas": [], "tweets_analyzed": len(tweets)}
    except Exception as e:
        logger.error(f"Strategic evaluation failed for {state}: {e}")
        return {"state": state, "score": 0.0, "level": "NORMAL", "conditions": [], "overall_assessment": "", "risk_areas": [], "tweets_analyzed": len(tweets)}


async def run_strategic(settings: Settings, run_id: int) -> dict:
    """Stage 3B: Evaluate strategic conditions for all states with sufficient data."""
    if not settings.openai_api_key:
        logger.warning("Stage 3B: No OpenAI API key — skipping strategic assessment")
        return {"states_assessed": 0}

    db = await get_session()
    try:
        now = datetime.utcnow()
        window_start = now - timedelta(days=settings.pipeline_strategic_window_days)

        # Find states with enough classified tweets in the 90-day window
        result = await db.execute(
            select(TwitterPost.ai_state, func.count(TwitterPost.id).label("cnt"))
            .where(
                TwitterPost.ai_classified_at.isnot(None),
                TwitterPost.posted_at >= window_start,
            )
            .group_by(TwitterPost.ai_state)
            .having(func.count(TwitterPost.id) >= settings.pipeline_strategic_min_tweets)
        )
        states = [(r[0], r[1]) for r in result.all() if r[0] and r[0] != "Unknown"]

        logger.info(f"Stage 3B: Evaluating strategic conditions for {len(states)} states...")
        assessed = 0

        for state_name, tweet_count in states:
            result = await evaluate_state_conditions(db, settings, state_name)

            # Update StateThreatLevel
            stl_result = await db.execute(
                select(StateThreatLevel).where(StateThreatLevel.state == state_name)
            )
            stl = stl_result.scalar_one_or_none()

            if stl:
                stl.strategic_score = result["score"]
                stl.strategic_level = result["level"]
                stl.strategic_conditions = result["conditions"]
                stl.strategic_assessed_at = now

                # Combined level = max(tactical, strategic)
                tactical_order = LEVEL_ORDER.get(stl.threat_level, 0)
                strategic_order = LEVEL_ORDER.get(result["level"], 0)
                combined_order = max(tactical_order, strategic_order)
                stl.combined_level = {v: k for k, v in LEVEL_ORDER.items()}[combined_order]

                # If strategic elevates above tactical, flag for reassessment
                if strategic_order > tactical_order:
                    stl.needs_assessment = True

                stl.updated_at = now
                await db.commit()

                if result["score"] > 0:
                    logger.info(
                        f"  {state_name}: strategic={result['level']} (score {result['score']:.1f}), "
                        f"tactical={stl.threat_level}, combined={stl.combined_level}, "
                        f"conditions: {[c['condition'] for c in result['conditions'] if c.get('present')]}"
                    )

            assessed += 1

        logger.info(f"Stage 3B complete: {assessed} states assessed")
        return {"states_assessed": assessed}
    finally:
        await db.close()
