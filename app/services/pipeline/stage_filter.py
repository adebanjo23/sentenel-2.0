"""Stage 1: Filter — discard noise, keep Nigeria security-relevant tweets."""

import logging
from datetime import datetime

from openai import AsyncOpenAI
from sqlalchemy import select, update

from app.config import Settings
from app.database import get_session
from app.models import TwitterPost

logger = logging.getLogger("sentinel.pipeline.filter")

INCLUDE_KEYWORDS = [
    "attack", "killed", "kidnap", "abduct", "bandit", "terrorist",
    "boko haram", "iswap", "insurgent", "military operation", "airstrike",
    "gunmen", "armed men", "ambush", "explosion", "bomb", "ied",
    "troops", "soldiers", "army", "airforce", "police",
    "herder", "farmer", "communal", "clash",
    "displaced", "idp", "refugee",
    "arrest", "weapon", "arms cache", "ammunition", "gunrunner",
    "protest", "riot", "unrest", "curfew",
    "massacre", "slaughter", "casualt", "fatali",
    "security", "insecurity", "threat", "terror",
    "operation hadin kai", "operation whirl stroke", "operation fansan",
    "operation enduring peace", "operation safe haven",
    # Hausa / Pidgin
    "yaki", "gwagwarwa", "soja", "mutuwa",
]

EXCLUDE_KEYWORDS = [
    "premier league", "champions league", "la liga", "serie a",
    "nba ", "nfl ", "world cup", "olympics",
    "big brother naija", "bbnaija", "nollywood",
    "bitcoin", "crypto", "forex", "trading signal",
    "giveaway", "promo code",
]

# Nigerian states — tweets mentioning these + a security keyword are likely relevant
NIGERIAN_STATES = [
    "abia", "adamawa", "akwa ibom", "anambra", "bauchi", "bayelsa", "benue",
    "borno", "cross river", "delta", "ebonyi", "edo", "ekiti", "enugu",
    "gombe", "imo", "jigawa", "kaduna", "kano", "katsina", "kebbi",
    "kogi", "kwara", "lagos", "nasarawa", "niger", "ogun", "ondo",
    "osun", "oyo", "plateau", "rivers", "sokoto", "taraba", "yobe", "zamfara",
    "fct", "abuja",
]


def keyword_check(content: str) -> str | None:
    """Fast keyword-based pre-filter. Returns 'include', 'exclude', or None (borderline)."""
    if not content:
        return "exclude"

    lower = content.lower()

    for kw in EXCLUDE_KEYWORDS:
        if kw in lower:
            # Could still be relevant if it also has security keywords
            has_security = any(sk in lower for sk in INCLUDE_KEYWORDS[:20])
            if not has_security:
                return "exclude"

    for kw in INCLUDE_KEYWORDS:
        if kw in lower:
            return "include"

    # Check for state name + any vaguely security-related term
    has_state = any(s in lower for s in NIGERIAN_STATES)
    if has_state:
        weak_signals = ["dead", "fire", "burn", "flee", "rescue", "danger", "alarm", "warning"]
        if any(w in lower for w in weak_signals):
            return "include"

    return None  # borderline


FILTER_PROMPT = """You are a filter for a Nigerian security intelligence system.
For each tweet, respond with ONLY "Y" (security-relevant to Nigeria) or "N" (noise).

Security-relevant: reports of attacks, kidnappings, military operations, armed conflict,
communal violence, displacement, protests/unrest, arrests of militants, arms trafficking,
credible threats, security warnings — all within Nigeria or its border regions.

NOT relevant: international news not about Nigeria, Nigerian politics without security angle,
sports, entertainment, personal opinions without incident info, business/economy news.

Tweets:
{tweets}

Respond with one line per tweet: the tweet ID followed by Y or N. Nothing else."""


async def ai_filter_batch(tweets: list[dict], api_key: str, model: str) -> dict[str, bool]:
    """Send borderline tweets to GPT-4.1-mini for relevance check."""
    client = AsyncOpenAI(api_key=api_key)

    tweet_text = ""
    for t in tweets:
        tweet_text += f"[{t['tweet_id']}] {(t['content'] or '')[:280]}\n"

    try:
        response = await client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "Respond only with tweet ID followed by Y or N, one per line."},
                {"role": "user", "content": FILTER_PROMPT.format(tweets=tweet_text)},
            ],
            temperature=0.0,
        )
        raw = response.choices[0].message.content.strip()
        results = {}
        for line in raw.split("\n"):
            line = line.strip()
            if not line:
                continue
            # Parse "ID Y" or "ID: Y" or "[ID] Y" formats
            cleaned = line.replace("[", "").replace("]", "").replace(":", " ")
            parts = cleaned.split()
            if len(parts) >= 2:
                tid = parts[0]
                verdict = parts[-1].upper()
                results[tid] = verdict == "Y"
        return results
    except Exception as e:
        logger.error(f"AI filter failed: {e}")
        return {t["tweet_id"]: True for t in tweets}


async def run_filter(settings: Settings, run_id: int) -> dict:
    """Stage 1: Filter unprocessed tweets into security-relevant and noise."""
    db = await get_session()
    try:
        result = await db.execute(
            select(TwitterPost)
            .where(TwitterPost.pipeline_status.is_(None))
            .order_by(TwitterPost.posted_at.desc())
            .limit(settings.pipeline_max_tweets_per_run)
        )
        tweets = result.scalars().all()

        if not tweets:
            logger.info("Stage 1: No unprocessed tweets")
            return {"tweets_in": 0, "passed": 0, "filtered": 0}

        logger.info(f"Stage 1: Filtering {len(tweets)} tweets...")
        passed = 0
        filtered = 0
        borderline = []

        # Pass 1: keyword filter
        for t in tweets:
            verdict = keyword_check(t.content or "")
            if verdict == "include":
                await db.execute(
                    update(TwitterPost).where(TwitterPost.id == t.id).values(
                        pipeline_status="filtered_in",
                        pipeline_filter_method="keyword",
                        pipeline_run_id=run_id,
                    )
                )
                passed += 1
            elif verdict == "exclude":
                await db.execute(
                    update(TwitterPost).where(TwitterPost.id == t.id).values(
                        pipeline_status="filtered_out",
                        pipeline_filter_method="keyword",
                        pipeline_run_id=run_id,
                    )
                )
                filtered += 1
            else:
                borderline.append({"id": t.id, "tweet_id": t.tweet_id, "content": t.content})

        await db.commit()
        logger.info(f"Stage 1 keywords: {passed} in, {filtered} out, {len(borderline)} borderline")

        # Pass 2: AI filter for borderline tweets
        if borderline and settings.openai_api_key:
            batch_size = settings.pipeline_filter_batch_size
            for i in range(0, len(borderline), batch_size):
                batch = borderline[i:i + batch_size]
                ai_results = await ai_filter_batch(batch, settings.openai_api_key, settings.pipeline_model_mini)
                for bt in batch:
                    is_relevant = ai_results.get(bt["tweet_id"], True)
                    status = "filtered_in" if is_relevant else "filtered_out"
                    await db.execute(
                        update(TwitterPost).where(TwitterPost.id == bt["id"]).values(
                            pipeline_status=status,
                            pipeline_filter_method="ai",
                            pipeline_run_id=run_id,
                        )
                    )
                    if is_relevant:
                        passed += 1
                    else:
                        filtered += 1
            await db.commit()
        elif borderline:
            for bt in borderline:
                await db.execute(
                    update(TwitterPost).where(TwitterPost.id == bt["id"]).values(
                        pipeline_status="filtered_in",
                        pipeline_filter_method="default",
                        pipeline_run_id=run_id,
                    )
                )
                passed += 1
            await db.commit()

        logger.info(f"Stage 1 complete: {passed} passed, {filtered} filtered out of {len(tweets)}")
        return {"tweets_in": len(tweets), "passed": passed, "filtered": filtered}
    finally:
        await db.close()
