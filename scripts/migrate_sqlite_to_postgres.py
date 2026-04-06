"""Migrate all data from local SQLite to remote PostgreSQL."""

import asyncio
import json
import logging
import sqlite3
import sys
from datetime import date, datetime

import asyncpg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger("migrate")


def get_pg_url():
    if len(sys.argv) > 1:
        url = sys.argv[1]
        return url.replace("postgresql+asyncpg://", "postgresql://")

    try:
        with open(".env") as f:
            for line in f:
                line = line.strip()
                if line.startswith("DATABASE_URL=") and "postgresql" in line:
                    url = line.split("=", 1)[1]
                    return url.replace("postgresql+asyncpg://", "postgresql://")
    except FileNotFoundError:
        pass

    print("Usage: python scripts/migrate_sqlite_to_postgres.py <POSTGRES_URL>")
    sys.exit(1)


def parse_datetime(val):
    """Convert string datetime to Python datetime."""
    if val is None:
        return None
    if isinstance(val, (datetime, date)):
        return val
    val = str(val).strip()
    if not val:
        return None
    for fmt in [
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ]:
        try:
            return datetime.strptime(val, fmt)
        except ValueError:
            continue
    return None


def parse_date(val):
    """Convert string date to Python date."""
    if val is None:
        return None
    if isinstance(val, date):
        return val
    val = str(val).strip()
    if not val:
        return None
    try:
        return datetime.strptime(val, "%Y-%m-%d").date()
    except ValueError:
        return None


def parse_bool(val):
    """Convert SQLite integer to Python bool."""
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    return bool(int(val))


def parse_json(val):
    """Ensure JSON columns are proper dicts/lists, not strings."""
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return json.dumps(val)
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            return json.dumps(parsed)
        except (json.JSONDecodeError, ValueError):
            return val
    return str(val)


# Column type mapping per table — which columns need conversion
# 'd' = date, 'dt' = datetime, 'b' = boolean, 'j' = json
TABLE_COLUMNS = {
    "acled_events": {
        "event_date": "d",
        "ingested_at": "dt",
        "updated_at": "dt",
        "raw_json": "j",
    },
    "firms_hotspots": {
        "acq_date": "d",
        "ingested_at": "dt",
    },
    "tiktok_videos": {
        "posted_at": "dt",
        "collected_at": "dt",
        "video_downloaded": "b",
        "ai_processed_at": "dt",
        "raw_json": "j",
    },
    "twitter_posts": {
        "verified": "b",
        "posted_at": "dt",
        "collected_at": "dt",
        "is_retweet": "b",
        "is_reply": "b",
        "has_media": "b",
        "media_urls": "j",
        "ai_processed_at": "dt",
        "ai_classified_at": "dt",
        "ai_is_original_report": "b",
        "raw_json": "j",
    },
    "sync_runs": {
        "started_at": "dt",
        "completed_at": "dt",
        "run_metadata": "j",
    },
    "events": {
        "event_time": "dt",
        "first_reported": "dt",
        "last_updated": "dt",
        "created_at": "dt",
    },
    "event_sources": {
        "added_at": "dt",
    },
    "pipeline_runs": {
        "started_at": "dt",
        "completed_at": "dt",
        "stage1_completed_at": "dt",
        "stage2_completed_at": "dt",
        "stage3_completed_at": "dt",
        "stage4_completed_at": "dt",
        "stage5_completed_at": "dt",
        "run_metadata": "j",
    },
    "state_threat_levels": {
        "severity_distribution": "j",
        "category_mix": "j",
        "repeat_lgas": "j",
        "needs_assessment": "b",
        "last_assessment_at": "dt",
        "updated_at": "dt",
    },
    "threat_assessments": {
        "primary_threat_areas": "j",
        "key_indicators": "j",
        "specific_warnings": "j",
        "recommended_actions": "j",
        "events_referenced": "j",
        "created_at": "dt",
    },
    "threat_alerts": {
        "primary_threat_areas": "j",
        "recommended_actions": "j",
        "acknowledged": "b",
        "acknowledged_at": "dt",
        "created_at": "dt",
    },
}


def convert_row(row, columns, table_name):
    """Convert a SQLite row to PostgreSQL-compatible types."""
    type_map = TABLE_COLUMNS.get(table_name, {})
    result = []
    for i, val in enumerate(row):
        col_name = columns[i]
        col_type = type_map.get(col_name)

        if col_type == "d":
            val = parse_date(val)
        elif col_type == "dt":
            val = parse_datetime(val)
        elif col_type == "b":
            val = parse_bool(val)
        elif col_type == "j":
            val = parse_json(val)

        result.append(val)
    return tuple(result)


async def migrate():
    pg_url = get_pg_url()

    logger.info("Source: SQLite (./data/sentinel.db)")
    logger.info("Target: PostgreSQL")

    sqlite_conn = sqlite3.connect("./data/sentinel.db")

    # Create tables via SQLAlchemy
    logger.info("Creating PostgreSQL tables...")
    sys.path.insert(0, ".")
    from app.models import Base
    from sqlalchemy.ext.asyncio import create_async_engine

    sa_url = pg_url.replace("postgresql://", "postgresql+asyncpg://")
    sa_engine = create_async_engine(sa_url, echo=False)
    async with sa_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await sa_engine.dispose()
    logger.info("Tables created")

    pg_conn = await asyncpg.connect(pg_url)

    tables = [
        "acled_events", "firms_hotspots", "tiktok_videos", "twitter_posts",
        "sync_runs", "events", "event_sources", "pipeline_runs",
        "state_threat_levels", "threat_assessments", "threat_alerts",
    ]

    for table in tables:
        cursor = sqlite_conn.execute(f"SELECT * FROM {table} LIMIT 0")
        columns = [desc[0] for desc in cursor.description]

        count = sqlite_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if count == 0:
            logger.info(f"  {table}: 0 rows (skipped)")
            continue

        # Clear existing data in PostgreSQL
        pg_count = await pg_conn.fetchval(f"SELECT COUNT(*) FROM {table}")
        if pg_count > 0:
            logger.info(f"  {table}: clearing {pg_count} existing rows...")
            await pg_conn.execute(f"DELETE FROM {table}")

        # Read from SQLite
        cursor = sqlite_conn.execute(f"SELECT * FROM {table}")
        rows = cursor.fetchall()

        # Build INSERT
        placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
        col_names = ", ".join(f'"{c}"' for c in columns)
        insert_sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"

        logger.info(f"  {table}: migrating {len(rows)} rows...")

        inserted = 0
        errors = 0
        batch_size = 100

        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            converted = [convert_row(row, columns, table) for row in batch]

            try:
                await pg_conn.executemany(insert_sql, converted)
                inserted += len(converted)
            except Exception:
                # One by one fallback
                for row in converted:
                    try:
                        await pg_conn.execute(insert_sql, *row)
                        inserted += 1
                    except Exception as e:
                        errors += 1
                        if errors <= 3:
                            logger.warning(f"    Row error: {e}")

        logger.info(f"  {table}: {inserted} inserted, {errors} errors")

    # Fix sequences
    logger.info("Fixing sequences...")
    for table in tables:
        try:
            await pg_conn.execute(f"""
                SELECT setval(
                    pg_get_serial_sequence('{table}', 'id'),
                    COALESCE((SELECT MAX(id) FROM {table}), 1)
                )
            """)
        except Exception:
            pass

    sqlite_conn.close()
    await pg_conn.close()
    logger.info("Migration complete!")


if __name__ == "__main__":
    asyncio.run(migrate())
