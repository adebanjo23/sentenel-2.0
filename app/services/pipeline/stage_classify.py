"""Stage 2: Classify — tag each security-relevant tweet with structured intel fields."""

import json
import logging
from datetime import datetime

from openai import AsyncOpenAI
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.database import get_session
from app.models import TwitterPost, Event, EventSource
from app.services.intel_agent import find_existing_event, find_firms_matches, calculate_confidence

logger = logging.getLogger("sentinel.pipeline.classify")

CLASSIFY_PROMPT = """You are a Nigerian security intelligence analyst. Classify each tweet with structured fields.

For each tweet, extract:
- state: Nigerian state name (e.g. "Borno", "Plateau"). Use "Unknown" if unclear. If about multiple states, use the primary one.
- lga: Local Government Area if mentioned, else null
- incident_type: one of [attack, kidnapping, protest, military_operation, ied, airstrike, communal_violence, threat, displacement, arrest, armed_robbery, other]
- severity: one of [critical, high, moderate, low]
- category: one of [report, military_operation, warning, tension, arrest, displacement, arms_trafficking, reprisal]
  - "report" = reporting an incident that happened
  - "military_operation" = troops conducting operations
  - "warning" = a threat warning or alarm raised by a community
  - "tension" = escalating tensions, inflammatory statements
  - "arrest" = security forces arresting suspects
  - "displacement" = people fleeing/displaced
  - "arms_trafficking" = weapons seized, gun runners arrested
  - "reprisal" = retaliatory attack
- actors: list of groups/forces mentioned (e.g. ["ISWAP", "Nigerian Army"])
- fatalities_mentioned: integer or null
- is_original_report: true if first-hand report, false if retweet/commentary/follow-up
- summary: one sentence factual summary
- location_name: specific location if mentioned, else null

Group tweets about the SAME incident by giving them the same "event_group" integer (starting at 1).
Different incidents get different event_group numbers.

Respond with valid JSON: {{"classifications": [...]}}

TWEETS:
{tweets}"""


async def classify_batch(tweets: list[dict], api_key: str, model: str) -> list[dict]:
    """Send a batch of tweets to GPT-4.1-mini for classification."""
    client = AsyncOpenAI(api_key=api_key)

    tweet_text = ""
    for t in tweets:
        tweet_text += f"[ID: {t['tweet_id']}] @{t['author_handle'] or '?'}: {t['content']}\n"
        tweet_text += f"  Posted: {t['posted_at']}\n\n"

    try:
        response = await client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a security intelligence analyst. Always respond with valid JSON. No markdown, no code blocks — just raw JSON."},
                {"role": "user", "content": CLASSIFY_PROMPT.format(tweets=tweet_text)},
            ],
            temperature=0.2,
        )
        raw = response.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
            if raw.endswith("```"):
                raw = raw[:-3].strip()

        result = json.loads(raw)
        return result.get("classifications", [])
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse classification JSON: {e}")
        return []
    except Exception as e:
        logger.error(f"Classification batch failed: {e}")
        return []


async def apply_classifications(db: AsyncSession, classifications: list[dict]) -> None:
    """Write classification fields back to TwitterPost records."""
    for c in classifications:
        tweet_id = c.get("tweet_id")
        if not tweet_id:
            continue

        actors = c.get("actors", [])
        actors_str = ",".join(actors) if isinstance(actors, list) else actors

        await db.execute(
            update(TwitterPost).where(TwitterPost.tweet_id == str(tweet_id)).values(
                ai_event_type=c.get("incident_type"),
                ai_location=c.get("location_name"),
                ai_severity=c.get("severity"),
                ai_summary=c.get("summary"),
                ai_actors=actors_str,
                ai_processed_at=datetime.utcnow(),
                ai_state=c.get("state"),
                ai_lga=c.get("lga"),
                ai_incident_type=c.get("incident_type"),
                ai_category=c.get("category"),
                ai_fatalities_mentioned=c.get("fatalities_mentioned"),
                ai_is_original_report=c.get("is_original_report"),
                ai_classified_at=datetime.utcnow(),
            )
        )


async def create_events_from_groups(
    db: AsyncSession, classifications: list[dict], tweets_lookup: dict[str, TwitterPost],
) -> dict:
    """Group classified tweets into events and create/update Event records."""
    groups: dict[int, list[dict]] = {}
    for c in classifications:
        g = c.get("event_group", 0)
        if g not in groups:
            groups[g] = []
        groups[g].append(c)

    events_created = 0
    events_updated = 0

    for group_items in groups.values():
        rep = group_items[0]
        tweet_ids = [str(c.get("tweet_id", "")) for c in group_items if c.get("tweet_id")]

        if not tweet_ids:
            continue

        actors = rep.get("actors", [])
        event_data = {
            "event_type": rep.get("incident_type"),
            "severity": rep.get("severity"),
            "location_name": rep.get("location_name"),
            "admin1": rep.get("state"),
            "admin2": rep.get("lga"),
            "latitude": None,
            "longitude": None,
        }

        try:
            existing = await find_existing_event(db, event_data)

            if existing:
                existing.twitter_sources += len(tweet_ids)
                existing.last_updated = datetime.utcnow()

                score, label = calculate_confidence(
                    existing.twitter_sources, existing.firms_sources, existing.acled_sources
                )
                existing.confidence_score = score
                existing.confidence_label = label

                if rep.get("summary") and len(rep["summary"]) > len(existing.summary or ""):
                    existing.summary = rep["summary"]

                for tid in tweet_ids:
                    es = EventSource(event_id=existing.id, source_type="twitter", source_id=tid)
                    db.add(es)

                await db.commit()
                events_updated += 1
            else:
                twitter_count = len(tweet_ids)
                firms_count = 0

                score, label = calculate_confidence(twitter_count, firms_count, 0)

                # Find earliest tweet time
                first_tweet = None
                for tid in tweet_ids:
                    t = tweets_lookup.get(tid)
                    if t and t.posted_at and (first_tweet is None or t.posted_at < first_tweet):
                        first_tweet = t.posted_at

                actors_str = ",".join(actors) if isinstance(actors, list) else (actors or "")

                event = Event(
                    title=(rep.get("summary") or "Unknown event")[:300],
                    event_type=rep.get("incident_type"),
                    severity=rep.get("severity"),
                    confidence_score=score,
                    confidence_label=label,
                    location_name=rep.get("location_name"),
                    admin1=rep.get("state"),
                    admin2=rep.get("lga"),
                    summary=rep.get("summary"),
                    actors=actors_str,
                    fatality_estimate=rep.get("fatalities_mentioned"),
                    twitter_sources=twitter_count,
                    firms_sources=firms_count,
                    acled_sources=0,
                    first_reported=first_tweet,
                    event_time=first_tweet,
                )
                db.add(event)
                await db.commit()
                await db.refresh(event)

                for tid in tweet_ids:
                    es = EventSource(event_id=event.id, source_type="twitter", source_id=tid)
                    db.add(es)
                await db.commit()
                events_created += 1

        except Exception as e:
            logger.error(f"Error creating event from group: {e}")
            await db.rollback()

    return {"events_created": events_created, "events_updated": events_updated}


async def run_classify(settings: Settings, run_id: int) -> dict:
    """Stage 2: Classify filtered tweets and create events."""
    db = await get_session()
    try:
        result = await db.execute(
            select(TwitterPost)
            .where(
                TwitterPost.pipeline_status == "filtered_in",
                TwitterPost.ai_classified_at.is_(None),
            )
            .order_by(TwitterPost.posted_at.desc())
            .limit(settings.pipeline_max_tweets_per_run)
        )
        tweets = result.scalars().all()

        if not tweets:
            logger.info("Stage 2: No tweets to classify")
            return {"classified": 0, "events_created": 0, "events_updated": 0}

        logger.info(f"Stage 2: Classifying {len(tweets)} tweets...")

        # Build lookup
        tweets_lookup = {t.tweet_id: t for t in tweets}

        total_classified = 0
        total_events_created = 0
        total_events_updated = 0

        batch_size = settings.pipeline_classify_batch_size
        tweet_dicts = [
            {
                "tweet_id": t.tweet_id,
                "author_handle": t.author_handle,
                "content": t.content,
                "posted_at": t.posted_at.isoformat() if t.posted_at else None,
            }
            for t in tweets
        ]

        for i in range(0, len(tweet_dicts), batch_size):
            batch = tweet_dicts[i:i + batch_size]
            logger.info(f"  Classifying batch {i // batch_size + 1} ({len(batch)} tweets)...")

            classifications = await classify_batch(
                batch, settings.openai_api_key, settings.pipeline_model_mini
            )

            if classifications:
                await apply_classifications(db, classifications)
                event_stats = await create_events_from_groups(db, classifications, tweets_lookup)
                total_classified += len(classifications)
                total_events_created += event_stats["events_created"]
                total_events_updated += event_stats["events_updated"]

            await db.commit()

        logger.info(
            f"Stage 2 complete: {total_classified} classified, "
            f"{total_events_created} events created, {total_events_updated} updated"
        )
        return {
            "classified": total_classified,
            "events_created": total_events_created,
            "events_updated": total_events_updated,
        }
    finally:
        await db.close()
