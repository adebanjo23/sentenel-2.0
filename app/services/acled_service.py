"""ACLED service — fetch conflict events via OAuth API + store in DB."""

import logging
from datetime import datetime, date, timedelta

import requests
from sqlalchemy import select, update, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.database import get_session
from app.exceptions import APIError, AuthenticationError, RateLimitError
from app.models import ACLEDEvent, SyncRun

logger = logging.getLogger("sentinel.acled")


# ---------------------------------------------------------------------------
# API client functions
# ---------------------------------------------------------------------------

def _authenticate(settings: Settings) -> str:
    """Obtain an OAuth access token from ACLED. Returns the token string."""
    try:
        resp = requests.post(
            settings.acled_token_url,
            data={
                "grant_type": "password",
                "client_id": "acled",
                "username": settings.acled_email,
                "password": settings.acled_password,
            },
            timeout=60,
        )
    except requests.RequestException as e:
        raise AuthenticationError(f"OAuth request failed: {e}")

    if resp.status_code in (401, 403):
        raise AuthenticationError(
            "Invalid ACLED credentials. Check ACLED_EMAIL and ACLED_PASSWORD in .env"
        )
    if resp.status_code != 200:
        raise AuthenticationError(f"ACLED OAuth returned HTTP {resp.status_code}: {resp.text[:200]}")

    token = resp.json().get("access_token")
    if not token:
        raise AuthenticationError("No access_token in OAuth response")

    logger.info("ACLED OAuth authentication successful")
    return token


def _request(settings: Settings, params: dict, token: str) -> tuple[dict, str]:
    """Make an authenticated request. Returns (data, token) — token may be refreshed."""
    headers = {"Authorization": f"Bearer {token}"}

    try:
        resp = requests.get(settings.acled_base_url, params=params, headers=headers, timeout=60)
    except requests.RequestException as e:
        raise APIError(f"Request failed: {e}")

    # Token expired — re-authenticate once
    if resp.status_code == 401:
        logger.info("Token expired, re-authenticating")
        token = _authenticate(settings)
        headers = {"Authorization": f"Bearer {token}"}
        try:
            resp = requests.get(settings.acled_base_url, params=params, headers=headers, timeout=60)
        except requests.RequestException as e:
            raise APIError(f"Request failed after re-auth: {e}")
        if resp.status_code == 401:
            raise AuthenticationError("Authentication failed after token refresh.")

    if resp.status_code == 429:
        raise RateLimitError("ACLED rate limit exceeded.")
    if resp.status_code != 200:
        raise APIError(f"ACLED API returned HTTP {resp.status_code}: {resp.text[:200]}", resp.status_code)

    data = resp.json()
    if not data.get("success", True):
        raise APIError(f"ACLED API error: {data.get('messages', [])}")

    return data, token


def fetch_events(
    settings: Settings,
    date_start: str | None = None,
    date_end: str | None = None,
) -> list[dict]:
    """Fetch events with automatic pagination. Returns list of event dicts."""
    token = _authenticate(settings)
    all_events = []
    page = 1

    while True:
        params: dict = {
            "_format": "json",
            "country": settings.acled_country,
            "limit": settings.acled_page_limit,
            "page": page,
        }
        if date_start and date_end:
            params["event_date"] = f"{date_start}|{date_end}"
            params["event_date_where"] = "BETWEEN"
        elif date_start:
            params["event_date"] = date_start
            params["event_date_where"] = ">="

        data, token = _request(settings, params, token)
        events = data.get("data", [])
        if not events:
            break

        all_events.extend(events)
        logger.info(f"Page {page}: {len(events)} events (total: {len(all_events)})")

        if len(events) < settings.acled_page_limit:
            break
        page += 1

    return all_events


# ---------------------------------------------------------------------------
# Storage functions
# ---------------------------------------------------------------------------

def _parse_date(val):
    if isinstance(val, str):
        try:
            return date.fromisoformat(val)
        except ValueError:
            return None
    return val


async def save_events(db: AsyncSession, events: list[dict]) -> dict:
    """Save events to DB. Returns stats dict."""
    stats = {"new": 0, "updated": 0, "duplicate": 0, "error": 0}

    for event_data in events:
        try:
            event_id = event_data.get("event_id_cnty")
            if not event_id:
                stats["error"] += 1
                continue

            result = await db.execute(
                select(ACLEDEvent).where(ACLEDEvent.event_id == event_id)
            )
            existing = result.scalar_one_or_none()

            lat = event_data.get("latitude")
            lon = event_data.get("longitude")

            # Build serializable raw_json
            raw_json = {}
            for k, v in event_data.items():
                if isinstance(v, (datetime, date)):
                    raw_json[k] = v.isoformat()
                else:
                    raw_json[k] = v

            fields = dict(
                event_date=_parse_date(event_data.get("event_date")),
                year=event_data.get("year"),
                event_type=event_data.get("event_type"),
                sub_event_type=event_data.get("sub_event_type"),
                disorder_type=event_data.get("disorder_type"),
                actor1=event_data.get("actor1"),
                actor2=event_data.get("actor2"),
                interaction=event_data.get("interaction"),
                country=event_data.get("country", "Nigeria"),
                admin1=event_data.get("admin1"),
                admin2=event_data.get("admin2"),
                admin3=event_data.get("admin3"),
                location_name=event_data.get("location"),
                latitude=lat,
                longitude=lon,
                geo_precision=event_data.get("geo_precision"),
                source=event_data.get("source"),
                notes=event_data.get("notes"),
                fatalities=event_data.get("fatalities", 0),
                tags=event_data.get("tags"),
                timestamp=event_data.get("timestamp"),
                raw_json=raw_json,
            )

            if existing:
                if existing.raw_json != event_data:
                    await db.execute(
                        update(ACLEDEvent)
                        .where(ACLEDEvent.event_id == event_id)
                        .values(**fields, updated_at=datetime.utcnow())
                    )
                    await db.commit()
                    stats["updated"] += 1
                else:
                    stats["duplicate"] += 1
            else:
                event = ACLEDEvent(event_id=event_id, **fields, ingested_at=datetime.utcnow())
                db.add(event)
                await db.commit()
                stats["new"] += 1

        except Exception as e:
            logger.error(f"Error saving event: {e}")
            await db.rollback()
            stats["error"] += 1

    logger.info(f"ACLED save: {stats}")
    return stats


async def get_last_event_date(db: AsyncSession) -> str | None:
    result = await db.execute(select(func.max(ACLEDEvent.event_date)))
    max_date = result.scalar()
    return max_date.strftime("%Y-%m-%d") if max_date else None


async def get_stats(db: AsyncSession) -> dict:
    total = (await db.execute(select(func.count(ACLEDEvent.id)))).scalar()

    dr = (await db.execute(
        select(func.min(ACLEDEvent.event_date), func.max(ACLEDEvent.event_date))
    )).one()

    types = (await db.execute(
        select(ACLEDEvent.event_type, func.count(ACLEDEvent.id).label("count"))
        .group_by(ACLEDEvent.event_type).order_by(desc("count"))
    )).all()

    states = (await db.execute(
        select(ACLEDEvent.admin1, func.count(ACLEDEvent.id).label("count"))
        .where(ACLEDEvent.admin1.isnot(None))
        .group_by(ACLEDEvent.admin1).order_by(desc("count")).limit(10)
    )).all()

    fatalities = (await db.execute(
        select(func.coalesce(func.sum(ACLEDEvent.fatalities), 0))
    )).scalar()

    runs = (await db.execute(
        select(SyncRun).where(SyncRun.source == "acled")
        .order_by(desc(SyncRun.id)).limit(5)
    )).scalars().all()

    return {
        "total_events": total,
        "date_range": {
            "min": dr[0].strftime("%Y-%m-%d") if dr[0] else None,
            "max": dr[1].strftime("%Y-%m-%d") if dr[1] else None,
        },
        "event_types": {r.event_type: r.count for r in types},
        "top_states": {r.admin1: r.count for r in states},
        "total_fatalities": fatalities,
        "recent_runs": [
            {
                "id": r.id,
                "started_at": r.started_at.isoformat(),
                "completed_at": r.completed_at.isoformat() if r.completed_at else None,
                "status": r.status,
                "records_new": r.records_new,
            }
            for r in runs
        ],
    }


# ---------------------------------------------------------------------------
# Orchestration — called from route background task
# ---------------------------------------------------------------------------

async def sync(settings: Settings, since: str | None = None, historical: bool = False):
    """Full sync: determine date range, fetch from API, save to DB."""
    db = await get_session()
    try:
        today = datetime.now().strftime("%Y-%m-%d")

        if historical:
            start = (datetime.now() - timedelta(days=settings.acled_historical_years * 365)).strftime("%Y-%m-%d")
        elif since:
            start = since
        else:
            last = await get_last_event_date(db)
            start = last or (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

        # Track run
        run = SyncRun(
            source="acled", status="running",
            run_metadata={"date_start": start, "date_end": today},
        )
        db.add(run)
        await db.commit()
        await db.refresh(run)

        logger.info(f"ACLED sync: {start} → {today}")
        events = fetch_events(settings, date_start=start, date_end=today)
        stats = await save_events(db, events)

        # Complete run
        await db.execute(
            update(SyncRun).where(SyncRun.id == run.id).values(
                completed_at=datetime.utcnow(), status="completed",
                records_fetched=len(events), records_new=stats["new"],
                records_updated=stats.get("updated", 0), records_error=stats["error"],
            )
        )
        await db.commit()
        logger.info(f"ACLED sync complete: {stats}")

    except Exception as e:
        logger.error(f"ACLED sync failed: {e}")
        try:
            await db.execute(
                update(SyncRun).where(SyncRun.id == run.id).values(
                    completed_at=datetime.utcnow(), status="failed", error_message=str(e)
                )
            )
            await db.commit()
        except Exception:
            pass
    finally:
        await db.close()
