"""FIRMS service — fetch satellite thermal data + store in DB."""

import csv
import io
import logging
from datetime import datetime, date

import requests
from sqlalchemy import select, update, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.database import get_session
from app.exceptions import APIError, AuthenticationError, RateLimitError
from app.models import FIRMSHotspot, SyncRun

logger = logging.getLogger("sentinel.firms")

CONFIDENCE_LEVELS = {"low": 0, "nominal": 1, "high": 2}


# ---------------------------------------------------------------------------
# API client functions
# ---------------------------------------------------------------------------

def _build_url(settings: Settings, source: str, day_range: int) -> str:
    return (
        f"{settings.firms_base_url}/api/area/csv/"
        f"{settings.firms_map_key}/{source}/{settings.firms_area_bbox}/{day_range}"
    )


def _filter_confidence(records: list[dict], min_confidence: str) -> list[dict]:
    min_level = CONFIDENCE_LEVELS.get(min_confidence, 0)
    filtered = []
    for rec in records:
        conf = rec.get("confidence", "").lower()
        if conf in CONFIDENCE_LEVELS:
            if CONFIDENCE_LEVELS[conf] >= min_level:
                filtered.append(rec)
        else:
            try:
                numeric = int(conf)
                if min_level == 0 or (min_level == 1 and numeric >= 30) or (min_level == 2 and numeric >= 80):
                    filtered.append(rec)
                else:
                    filtered.append(rec)
            except (ValueError, TypeError):
                filtered.append(rec)
    return filtered


MAX_DAYS_PER_REQUEST = 5


def _fetch_single_chunk(settings: Settings, source: str, day_range: int) -> list[dict]:
    """Fetch hotspots for a single source and day range (max 5 days)."""
    url = _build_url(settings, source, day_range)
    logger.debug(f"Fetching {source}, {day_range} days")

    try:
        resp = requests.get(url, timeout=60)
    except requests.RequestException as e:
        raise APIError(f"Request failed: {e}")

    if resp.status_code in (401, 403):
        raise AuthenticationError("Invalid FIRMS MAP key.")
    if resp.status_code == 429:
        raise RateLimitError()
    if resp.status_code != 200:
        raise APIError(f"FIRMS API returned HTTP {resp.status_code}: {resp.text[:200]}", resp.status_code)
    if resp.text.strip().startswith("Invalid"):
        raise APIError(f"FIRMS API error: {resp.text.strip()[:200]}")

    if not resp.text.strip():
        return []

    records = list(csv.DictReader(io.StringIO(resp.text)))
    logger.info(f"{source}: {len(records)} raw hotspots ({day_range}d)")

    for r in records:
        r["_source_sensor"] = source

    filtered = _filter_confidence(records, settings.firms_min_confidence)
    logger.info(f"{source}: {len(filtered)} after confidence filter (>= {settings.firms_min_confidence})")
    return filtered


def fetch_hotspots(settings: Settings, source: str, day_range: int) -> list[dict]:
    """Fetch hotspots, automatically chunking if days > 5."""
    if day_range <= MAX_DAYS_PER_REQUEST:
        return _fetch_single_chunk(settings, source, day_range)

    # Chunk into multiple requests of max 5 days each
    all_records = []
    remaining = day_range
    while remaining > 0:
        chunk = min(remaining, MAX_DAYS_PER_REQUEST)
        try:
            records = _fetch_single_chunk(settings, source, chunk)
            all_records.extend(records)
        except APIError as e:
            logger.error(f"Chunk failed ({chunk}d remaining of {day_range}d): {e}")
        remaining -= chunk

    logger.info(f"{source}: {len(all_records)} total hotspots across {day_range} days")
    return all_records


def fetch_all_sources(settings: Settings, day_range: int) -> list[dict]:
    """Fetch from all configured sources."""
    sources = [s.strip() for s in settings.firms_sources.split(",")]
    all_hotspots = []
    for source in sources:
        try:
            hotspots = fetch_hotspots(settings, source, day_range)
            all_hotspots.extend(hotspots)
        except APIError as e:
            logger.error(f"Failed to fetch {source}: {e}")
    logger.info(f"Total from all sources: {len(all_hotspots)}")
    return all_hotspots


# ---------------------------------------------------------------------------
# Storage functions
# ---------------------------------------------------------------------------

def _parse_date(val):
    if isinstance(val, str):
        return date.fromisoformat(val)
    return val


async def save_hotspots(db: AsyncSession, records: list[dict]) -> dict:
    stats = {"new": 0, "duplicate": 0, "error": 0}
    for record in records:
        try:
            lat = float(record["latitude"])
            lon = float(record["longitude"])
            acq_date = _parse_date(record["acq_date"])

            # SQLite: manual duplicate check
            existing = await db.execute(
                select(FIRMSHotspot).where(
                    FIRMSHotspot.latitude == lat,
                    FIRMSHotspot.longitude == lon,
                    FIRMSHotspot.acq_date == acq_date,
                    FIRMSHotspot.acq_time == record["acq_time"],
                    FIRMSHotspot.source_sensor == record["_source_sensor"],
                )
            )
            if existing.scalar_one_or_none() is not None:
                stats["duplicate"] += 1
                continue

            hotspot = FIRMSHotspot(
                latitude=lat,
                longitude=lon,
                brightness=record.get("brightness") or record.get("bright_ti4"),
                scan=record.get("scan"),
                track=record.get("track"),
                acq_date=acq_date,
                acq_time=record["acq_time"],
                satellite=record.get("satellite"),
                instrument=record.get("instrument"),
                confidence=record.get("confidence"),
                frp=record.get("frp"),
                daynight=record.get("daynight"),
                source_sensor=record["_source_sensor"],
                ingested_at=datetime.utcnow(),
            )
            db.add(hotspot)
            await db.commit()
            stats["new"] += 1

        except Exception as e:
            logger.error(f"Error saving hotspot: {e}")
            await db.rollback()
            stats["error"] += 1

    logger.info(f"FIRMS save: {stats}")
    return stats


async def get_stats(db: AsyncSession) -> dict:
    total = (await db.execute(select(func.count(FIRMSHotspot.id)))).scalar()

    dr = (await db.execute(
        select(func.min(FIRMSHotspot.acq_date), func.max(FIRMSHotspot.acq_date))
    )).one()

    by_sensor = {
        r.source_sensor: r.count
        for r in (await db.execute(
            select(FIRMSHotspot.source_sensor, func.count(FIRMSHotspot.id).label("count"))
            .group_by(FIRMSHotspot.source_sensor).order_by(desc("count"))
        )).all()
    }

    by_confidence = {
        r.confidence: r.count
        for r in (await db.execute(
            select(FIRMSHotspot.confidence, func.count(FIRMSHotspot.id).label("count"))
            .group_by(FIRMSHotspot.confidence).order_by(desc("count"))
        )).all()
    }

    return {
        "total_hotspots": total,
        "date_range": {
            "min": dr[0].strftime("%Y-%m-%d") if dr[0] else None,
            "max": dr[1].strftime("%Y-%m-%d") if dr[1] else None,
        },
        "by_sensor": by_sensor,
        "by_confidence": by_confidence,
    }


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

async def sync(
    settings: Settings,
    source: str | None = None,
    days: int = 2,
    all_sources: bool = False,
):
    """Fetch hotspots and save to DB."""
    db = await get_session()
    try:
        run = SyncRun(
            source="firms", status="running",
            run_metadata={"source": source, "days": days, "all_sources": all_sources},
        )
        db.add(run)
        await db.commit()
        await db.refresh(run)

        if all_sources:
            records = fetch_all_sources(settings, days)
        else:
            src = source or settings.firms_sources.split(",")[0].strip()
            records = fetch_hotspots(settings, src, days)

        stats = await save_hotspots(db, records)

        await db.execute(
            update(SyncRun).where(SyncRun.id == run.id).values(
                completed_at=datetime.utcnow(), status="completed",
                records_fetched=len(records), records_new=stats["new"],
                records_error=stats["error"],
            )
        )
        await db.commit()
        logger.info(f"FIRMS sync complete: {stats}")

    except Exception as e:
        logger.error(f"FIRMS sync failed: {e}")
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
