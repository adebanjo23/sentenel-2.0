"""Stage 1: Filter — discard noise, keep Nigeria security-relevant tweets."""

import logging
from datetime import datetime

from openai import AsyncOpenAI
from sqlalchemy import select, update

from app.config import Settings
from app.database import get_session
from app.models import TwitterPost

logger = logging.getLogger("sentinel.pipeline.filter")

SECURITY_KEYWORDS = [
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

# Nigeria-specific terms — if these appear WITH security keywords, auto-include
NIGERIA_CONTEXT = [
    "nigeria", "nigerian",
    # States
    "abia", "adamawa", "akwa ibom", "anambra", "bauchi", "bayelsa", "benue",
    "borno", "cross river", "delta", "ebonyi", "edo", "ekiti", "enugu",
    "gombe", "imo", "jigawa", "kaduna", "kano", "katsina", "kebbi",
    "kogi", "kwara", "lagos", "nasarawa", "niger state", "ogun", "ondo",
    "osun", "oyo", "plateau", "rivers", "sokoto", "taraba", "yobe", "zamfara",
    "fct", "abuja", "maiduguri", "jos", "katsina",
    # Nigeria-specific actors and terms
    "boko haram", "iswap", "ipob", "ess-n", "nurtw",
    "nigerian army", "nigerian police", "nigerian airforce", "nscdc",
    "dhq", "hqnigerian", "operation hadin kai", "operation whirl stroke",
    "operation fansan", "operation enduring peace", "cjtf",
    "lga", "senatorial", "naira",
]

EXCLUDE_KEYWORDS = [
    "premier league", "champions league", "la liga", "serie a",
    "nba ", "nfl ", "world cup", "olympics",
    "big brother naija", "bbnaija", "nollywood",
    "bitcoin", "crypto", "forex", "trading signal",
    "giveaway", "promo code",
]

# International terms — if these appear WITHOUT Nigeria context, likely not about Nigeria
INTERNATIONAL_SIGNALS = [
    "gaza", "israel", "palestine", "hamas", "hezbollah",
    "iran", "tehran", "irgc", "hormuz",
    "ukraine", "russia", "kremlin",
    "sudan", "darfur", "rsf",
    "syria", "assad",
    "cameroon", "chad republic",
    "lebanon", "beirut",
    "pakistan", "kabul", "taliban",
    "yemen", "houthi",
    "somalia", "al-shabaab",
]


def keyword_check(content: str) -> str | None:
    """
    Fast keyword-based pre-filter.
    Returns:
      'include'  — has security keyword + Nigerian context → definitely relevant
      'exclude'  — matches exclude list or is clearly international
      None       — borderline, needs AI to decide
    """
    if not content:
        return "exclude"

    lower = content.lower()

    # Check explicit exclude keywords
    for kw in EXCLUDE_KEYWORDS:
        if kw in lower:
            return "exclude"

    has_security = any(kw in lower for kw in SECURITY_KEYWORDS)
    has_nigeria = any(ctx in lower for ctx in NIGERIA_CONTEXT)
    has_international = any(sig in lower for sig in INTERNATIONAL_SIGNALS)

    # Security keyword + Nigerian context = auto-include
    if has_security and has_nigeria and not has_international:
        return "include"

    # International content with no Nigerian context = auto-exclude
    if has_international and not has_nigeria:
        return "exclude"

    # Security keyword but no Nigerian context = borderline (send to AI)
    if has_security and not has_nigeria:
        return None

    # Nigerian context but no security keyword = borderline
    if has_nigeria and not has_security:
        return None

    # Nothing relevant at all
    if not has_security and not has_nigeria:
        return "exclude"

    # Both international AND Nigerian context (e.g. "Nigerians killed in Ukraine") = borderline
    return None


FILTER_PROMPT = """You are a filter for a Nigerian security intelligence system.
For each tweet, determine if it is about a SECURITY INCIDENT OR SECURITY SITUATION INSIDE NIGERIA.

Respond "Y" ONLY if the tweet is about something happening IN NIGERIA related to:
- Armed attacks, killings, kidnappings, bombings in Nigeria
- Nigerian military/police operations within Nigeria
- Arrests of criminals, terrorists, bandits in Nigeria
- Security warnings or threats within Nigerian states
- Communal violence, farmer-herder clashes in Nigeria
- Displacement of people within Nigeria
- Arms trafficking within Nigeria

Respond "N" if:
- The tweet is about events in OTHER COUNTRIES (Gaza, Iran, Israel, Sudan, Ukraine, Lebanon, etc.)
- Even if it mentions "killed" or "attack" — if it's not about Nigeria, mark N
- Nigerian politics, economy, sports, entertainment without security relevance
- General commentary or opinions without reporting a specific Nigerian security incident
- Events about Nigerians abroad (e.g. Nigerians in Ukraine, Nigerians in Libya) UNLESS it's about a security threat to Nigeria itself

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
