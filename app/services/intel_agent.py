"""Intelligence agent — classifies tweets, correlates events, cross-references FIRMS."""

import json
import logging
import math
from datetime import datetime, timedelta

from openai import AsyncOpenAI
from sqlalchemy import select, update, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings, get_settings
from app.database import get_session
from app.models import Event, EventSource, TwitterPost, FIRMSHotspot

logger = logging.getLogger("sentinel.intel")

CLASSIFICATION_PROMPT = """You are a Nigerian security intelligence analyst. Analyze these tweets and identify distinct security events.

For each event you identify, extract:
- title: short descriptive title (e.g. "ISWAP attack on military base in Malam Fatori")
- event_type: one of [attack, kidnapping, protest, military_operation, ied, airstrike, threat, displacement, other]
- severity: one of [critical, high, moderate, low]
- location_name: specific location mentioned
- admin1: Nigerian state (e.g. "Borno")
- admin2: LGA if mentioned
- latitude: approximate latitude if you can determine it (null if unsure)
- longitude: approximate longitude if you can determine it (null if unsure)
- actors: list of groups/forces involved
- fatality_estimate: number or null
- summary: 2-3 sentence factual summary
- tweet_ids: list of tweet IDs that relate to this event
- follow_up_searches: list of keyword searches that would find more info about this event

Group tweets about the SAME event together. Multiple tweets about one attack = one event with multiple tweet_ids.

Tweets that are NOT about security events (political commentary, general news, etc.) should be ignored.

Respond with valid JSON: {"events": [...]}

TWEETS:
{tweets}"""


# ---------------------------------------------------------------------------
# AI classification
# ---------------------------------------------------------------------------

async def classify_tweets(tweets: list[dict], api_key: str) -> list[dict]:
    """Send tweets to GPT-4 for classification. Returns list of extracted events."""
    if not api_key:
        logger.warning("No OpenAI API key — skipping classification")
        return []

    client = AsyncOpenAI(api_key=api_key)

    # Format tweets for the prompt
    tweet_text = ""
    for t in tweets:
        tweet_text += f"[ID: {t['tweet_id']}] @{t['author_handle'] or '?'}: {t['content']}\n"
        tweet_text += f"  Posted: {t['posted_at']}\n\n"

    try:
        response = await client.chat.completions.create(
            model="gpt-4.1",
            messages=[
                {"role": "system", "content": "You are a security intelligence analyst. Always respond with valid JSON. No markdown, no code blocks — just raw JSON."},
                {"role": "user", "content": CLASSIFICATION_PROMPT.format(tweets=tweet_text)},
            ],
            temperature=0.2,
        )

        raw_content = response.choices[0].message.content.strip()

        # Strip markdown code blocks if present
        if raw_content.startswith("```"):
            raw_content = raw_content.split("\n", 1)[1] if "\n" in raw_content else raw_content[3:]
            if raw_content.endswith("```"):
                raw_content = raw_content[:-3].strip()

        result = json.loads(raw_content)
        events = result.get("events", [])
        logger.info(f"AI classified {len(tweets)} tweets into {len(events)} events")
        return events

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse AI response as JSON: {e}")
        logger.error(f"Raw response: {raw_content[:500]}")
        return []
    except Exception as e:
        logger.error(f"Classification failed: {e}")
        return []


# ---------------------------------------------------------------------------
# FIRMS cross-reference
# ---------------------------------------------------------------------------

def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Rough distance in km between two points."""
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return 6371 * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


async def find_firms_matches(
    db: AsyncSession, lat: float, lon: float, radius_km: float = 25, hours: int = 48,
) -> list:
    """Find FIRMS hotspots near a location within a time window."""
    # Rough degree offset for the radius
    deg_offset = radius_km / 111.0
    cutoff = datetime.utcnow() - timedelta(hours=hours)

    result = await db.execute(
        select(FIRMSHotspot).where(
            FIRMSHotspot.latitude.between(lat - deg_offset, lat + deg_offset),
            FIRMSHotspot.longitude.between(lon - deg_offset, lon + deg_offset),
        )
    )
    hotspots = result.scalars().all()

    # Filter by actual distance
    matches = []
    for h in hotspots:
        dist = _distance_km(lat, lon, h.latitude, h.longitude)
        if dist <= radius_km:
            matches.append({"id": h.id, "distance_km": round(dist, 1), "date": str(h.acq_date), "confidence": h.confidence, "frp": h.frp})

    return matches


# ---------------------------------------------------------------------------
# Event correlation — find or create events
# ---------------------------------------------------------------------------

async def find_existing_event(db: AsyncSession, event_data: dict) -> Event | None:
    """Find an existing event that matches (same area, same timeframe, same type)."""
    lat = event_data.get("latitude")
    lon = event_data.get("longitude")
    admin1 = event_data.get("admin1")

    # Try coordinate match first (within 30km)
    if lat and lon:
        deg_offset = 30 / 111.0
        result = await db.execute(
            select(Event).where(
                Event.status == "active",
                Event.latitude.between(lat - deg_offset, lat + deg_offset),
                Event.longitude.between(lon - deg_offset, lon + deg_offset),
                Event.event_type == event_data.get("event_type"),
                Event.created_at >= datetime.utcnow() - timedelta(hours=48),
            )
        )
        existing = result.scalar_one_or_none()
        if existing:
            return existing

    # Fall back to state + event type match
    if admin1:
        result = await db.execute(
            select(Event).where(
                Event.status == "active",
                Event.admin1 == admin1,
                Event.event_type == event_data.get("event_type"),
                Event.created_at >= datetime.utcnow() - timedelta(hours=48),
            )
        )
        existing = result.scalar_one_or_none()
        if existing:
            return existing

    return None


def calculate_confidence(twitter_count: int, firms_count: int, acled_count: int) -> tuple[float, str]:
    """Calculate confidence score and label from source counts."""
    score = 0.0

    # Twitter
    if twitter_count >= 1:
        score += 0.15
    if twitter_count >= 3:
        score += 0.10
    if twitter_count >= 5:
        score += 0.10

    # FIRMS thermal confirmation
    if firms_count >= 1:
        score += 0.25

    # ACLED verification
    if acled_count >= 1:
        score += 0.30

    score = min(score, 1.0)

    if score >= 0.75:
        label = "confirmed"
    elif score >= 0.50:
        label = "high"
    elif score >= 0.25:
        label = "moderate"
    else:
        label = "unverified"

    return round(score, 2), label


# ---------------------------------------------------------------------------
# Main processing function
# ---------------------------------------------------------------------------

async def process_new_tweets(settings: Settings) -> int:
    """Process unanalyzed tweets: classify, correlate, cross-reference FIRMS."""
    db = await get_session()

    try:
        # Get tweets that haven't been processed by the intel agent yet
        result = await db.execute(
            select(TwitterPost)
            .where(TwitterPost.ai_processed_at.is_(None))
            .order_by(TwitterPost.posted_at.desc())
            .limit(100)
        )
        tweets = result.scalars().all()

        if not tweets:
            logger.info("No new tweets to process")
            return 0

        logger.info(f"Processing {len(tweets)} new tweets...")

        # Format for classification
        tweet_dicts = [
            {
                "tweet_id": t.tweet_id,
                "author_handle": t.author_handle,
                "content": t.content,
                "posted_at": t.posted_at.isoformat() if t.posted_at else None,
            }
            for t in tweets
        ]

        # Classify with AI
        classified_events = await classify_tweets(tweet_dicts, settings.openai_api_key)

        events_created = 0
        for ev in classified_events:
            try:
                # Check if this matches an existing event
                existing = await find_existing_event(db, ev)

                tweet_ids = ev.get("tweet_ids", [])

                if existing:
                    # Update existing event
                    existing.twitter_sources += len(tweet_ids)
                    existing.last_updated = datetime.utcnow()

                    # Recalculate confidence
                    score, label = calculate_confidence(
                        existing.twitter_sources, existing.firms_sources, existing.acled_sources
                    )
                    existing.confidence_score = score
                    existing.confidence_label = label

                    # Update summary if new one is better
                    if ev.get("summary") and len(ev["summary"]) > len(existing.summary or ""):
                        existing.summary = ev["summary"]

                    # Link new tweet sources
                    for tid in tweet_ids:
                        es = EventSource(event_id=existing.id, source_type="twitter", source_id=str(tid))
                        db.add(es)

                    await db.commit()
                    logger.info(f"Updated event #{existing.id}: '{existing.title}' (confidence: {label})")

                else:
                    # Create new event
                    twitter_count = len(tweet_ids)
                    firms_count = 0

                    # Cross-reference FIRMS
                    lat = ev.get("latitude")
                    lon = ev.get("longitude")
                    if lat and lon:
                        firms_matches = await find_firms_matches(db, float(lat), float(lon))
                        firms_count = len(firms_matches)
                        if firms_matches:
                            logger.info(f"  FIRMS match: {firms_count} hotspots within 25km")

                    score, label = calculate_confidence(twitter_count, firms_count, 0)

                    # Find earliest tweet time
                    first_tweet = None
                    for t in tweets:
                        if t.tweet_id in [str(tid) for tid in tweet_ids]:
                            if first_tweet is None or (t.posted_at and t.posted_at < first_tweet):
                                first_tweet = t.posted_at

                    event = Event(
                        title=ev.get("title", "Unknown event"),
                        event_type=ev.get("event_type"),
                        severity=ev.get("severity"),
                        confidence_score=score,
                        confidence_label=label,
                        location_name=ev.get("location_name"),
                        admin1=ev.get("admin1"),
                        admin2=ev.get("admin2"),
                        latitude=float(lat) if lat else None,
                        longitude=float(lon) if lon else None,
                        summary=ev.get("summary"),
                        actors=",".join(ev.get("actors", [])),
                        fatality_estimate=ev.get("fatality_estimate"),
                        twitter_sources=twitter_count,
                        firms_sources=firms_count,
                        acled_sources=0,
                        first_reported=first_tweet,
                        event_time=first_tweet,
                    )
                    db.add(event)
                    await db.commit()
                    await db.refresh(event)

                    # Link tweet sources
                    for tid in tweet_ids:
                        es = EventSource(event_id=event.id, source_type="twitter", source_id=str(tid))
                        db.add(es)

                    # Link FIRMS sources
                    if lat and lon:
                        for fm in firms_matches:
                            es = EventSource(event_id=event.id, source_type="firms", source_id=str(fm["id"]))
                            db.add(es)

                    await db.commit()
                    events_created += 1
                    logger.info(f"New event #{event.id}: '{event.title}' [{label}, {score}]")

            except Exception as e:
                logger.error(f"Error processing event: {e}")
                await db.rollback()

        # Mark all tweets as processed
        for t in tweets:
            await db.execute(
                update(TwitterPost)
                .where(TwitterPost.id == t.id)
                .values(ai_processed_at=datetime.utcnow())
            )
        await db.commit()

        logger.info(f"Processing complete: {events_created} new events from {len(tweets)} tweets")
        return events_created

    except Exception as e:
        logger.error(f"Tweet processing failed: {e}")
        return 0
    finally:
        await db.close()


# ---------------------------------------------------------------------------
# Query functions
# ---------------------------------------------------------------------------

async def get_events(
    db: AsyncSession,
    status: str = "active",
    severity: str | None = None,
    admin1: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Get intelligence events with filters."""
    query = select(Event).where(Event.status == status)

    if severity:
        query = query.where(Event.severity == severity)
    if admin1:
        query = query.where(Event.admin1 == admin1)

    query = query.order_by(desc(Event.last_updated)).limit(limit)
    result = await db.execute(query)
    events = result.scalars().all()

    return [
        {
            "id": e.id,
            "title": e.title,
            "event_type": e.event_type,
            "severity": e.severity,
            "confidence": e.confidence_label,
            "confidence_score": e.confidence_score,
            "location": e.location_name,
            "state": e.admin1,
            "lga": e.admin2,
            "latitude": e.latitude,
            "longitude": e.longitude,
            "summary": e.summary,
            "actors": e.actors,
            "fatality_estimate": e.fatality_estimate,
            "sources": {
                "twitter": e.twitter_sources,
                "firms": e.firms_sources,
                "acled": e.acled_sources,
            },
            "first_reported": e.first_reported.isoformat() if e.first_reported else None,
            "last_updated": e.last_updated.isoformat() if e.last_updated else None,
        }
        for e in events
    ]


async def get_event_detail(db: AsyncSession, event_id: int) -> dict | None:
    """Get a single event with its sources."""
    result = await db.execute(select(Event).where(Event.id == event_id))
    event = result.scalar_one_or_none()
    if not event:
        return None

    # Get linked sources
    sources_result = await db.execute(
        select(EventSource).where(EventSource.event_id == event_id)
    )
    sources = sources_result.scalars().all()

    # Get actual tweet content for linked tweets
    tweet_ids = [s.source_id for s in sources if s.source_type == "twitter"]
    linked_tweets = []
    if tweet_ids:
        tweets_result = await db.execute(
            select(TwitterPost).where(TwitterPost.tweet_id.in_(tweet_ids))
        )
        linked_tweets = [
            {
                "tweet_id": t.tweet_id,
                "author": t.author_handle,
                "content": t.content,
                "posted_at": t.posted_at.isoformat() if t.posted_at else None,
                "likes": t.likes,
                "retweets": t.retweets,
            }
            for t in tweets_result.scalars().all()
        ]

    return {
        "id": event.id,
        "title": event.title,
        "event_type": event.event_type,
        "severity": event.severity,
        "confidence": event.confidence_label,
        "confidence_score": event.confidence_score,
        "location": event.location_name,
        "state": event.admin1,
        "lga": event.admin2,
        "latitude": event.latitude,
        "longitude": event.longitude,
        "summary": event.summary,
        "actors": event.actors,
        "fatality_estimate": event.fatality_estimate,
        "sources": {
            "twitter": event.twitter_sources,
            "firms": event.firms_sources,
            "acled": event.acled_sources,
        },
        "first_reported": event.first_reported.isoformat() if event.first_reported else None,
        "last_updated": event.last_updated.isoformat() if event.last_updated else None,
        "tweets": linked_tweets,
    }
