"""Twitter service — scrape tweets via Playwright + store in DB."""

import logging
from datetime import datetime

from sqlalchemy import select, update, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.database import get_session
from app.models import TwitterPost, SyncRun
from app.services.twitter_scraper import PlaywrightScraper

logger = logging.getLogger("sentinel.twitter")


# ---------------------------------------------------------------------------
# Storage functions
# ---------------------------------------------------------------------------

async def save_tweet(db: AsyncSession, tweet_data: dict, search_query: str | None = None) -> bool:
    """Save a single tweet. Returns True if new."""
    try:
        tweet_id = tweet_data.get("id")
        if not tweet_id:
            return False

        existing = await db.execute(
            select(TwitterPost).where(TwitterPost.tweet_id == tweet_id)
        )
        if existing.scalar_one_or_none() is not None:
            return False

        # Parse posted_at
        created_at = tweet_data.get("created_at")
        posted_at = None
        if created_at:
            try:
                posted_at = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
                # Strip timezone for PostgreSQL TIMESTAMP WITHOUT TIME ZONE
                if posted_at.tzinfo is not None:
                    posted_at = posted_at.replace(tzinfo=None)
            except (ValueError, TypeError):
                pass

        # Media URLs
        media_urls = []
        for item in tweet_data.get("media", []):
            media_urls.append({"type": item.get("type"), "url": item.get("url")})

        user = tweet_data.get("user", {})
        metrics = tweet_data.get("metrics", {})

        view_count = metrics.get("view_count", 0)
        try:
            view_count = int(view_count) if view_count else 0
        except (ValueError, TypeError):
            view_count = 0

        post = TwitterPost(
            tweet_id=tweet_id,
            author_handle=user.get("username"),
            author_name=user.get("display_name"),
            followers_count=user.get("followers_count", 0),
            verified=user.get("verified", False),
            content=tweet_data.get("text"),
            language=tweet_data.get("lang"),
            posted_at=posted_at,
            collected_at=datetime.utcnow(),
            search_query=search_query,
            likes=metrics.get("favorite_count", 0),
            retweets=metrics.get("retweet_count", 0),
            replies=metrics.get("reply_count", 0),
            views=view_count,
            is_retweet=tweet_data.get("is_retweet", False),
            is_reply=tweet_data.get("is_reply", False),
            has_media=len(media_urls) > 0,
            media_urls=media_urls if media_urls else None,
            raw_json=tweet_data,
        )
        db.add(post)
        await db.commit()
        return True

    except Exception as e:
        logger.error(f"Error saving tweet {tweet_data.get('id', '?')}: {e}")
        await db.rollback()
        return False


async def save_tweets(db: AsyncSession, tweets: list[dict], search_query: str | None = None) -> dict:
    stats = {"new": 0, "duplicate": 0, "error": 0}
    for tweet in tweets:
        try:
            is_new = await save_tweet(db, tweet, search_query)
            stats["new" if is_new else "duplicate"] += 1
        except Exception:
            stats["error"] += 1
    logger.info(f"Twitter save: {stats}")
    return stats


async def get_stats(db: AsyncSession) -> dict:
    total = (await db.execute(select(func.count(TwitterPost.id)))).scalar()

    dr = (await db.execute(
        select(func.min(TwitterPost.posted_at), func.max(TwitterPost.posted_at))
    )).one()

    top_authors = {
        r.author_handle: r.count
        for r in (await db.execute(
            select(TwitterPost.author_handle, func.count(TwitterPost.id).label("count"))
            .where(TwitterPost.author_handle.isnot(None))
            .group_by(TwitterPost.author_handle).order_by(desc("count")).limit(10)
        )).all()
    }

    return {
        "total_posts": total,
        "date_range": {
            "min": dr[0].isoformat() if dr[0] else None,
            "max": dr[1].isoformat() if dr[1] else None,
        },
        "top_authors": top_authors,
    }


# ---------------------------------------------------------------------------
# Scraping orchestration
# ---------------------------------------------------------------------------

def _create_scraper(settings: Settings, headless: bool | None = None) -> PlaywrightScraper:
    return PlaywrightScraper(
        username=settings.twitter_username,
        password=settings.twitter_password,
        email=settings.twitter_email,
        headless=headless if headless is not None else settings.twitter_headless,
        scroll_delay_min=settings.twitter_scroll_delay_min,
        scroll_delay_max=settings.twitter_scroll_delay_max,
    )


async def search_tweets(settings: Settings, query: str, count: int | None = None, headless: bool | None = None):
    """Search Twitter and save results. Called from route background task."""
    db = await get_session()
    scraper = _create_scraper(settings, headless)

    try:
        if not await scraper.initialize():
            raise RuntimeError("Failed to initialize browser")
        if not await scraper.login():
            raise RuntimeError("Failed to login to Twitter")

        max_tweets = count or settings.twitter_default_count

        # Track run
        run = SyncRun(
            source="twitter", status="running",
            run_metadata={"query": query, "max_tweets": max_tweets},
        )
        db.add(run)
        await db.commit()
        await db.refresh(run)

        result = await scraper.search_tweets(query=query, max_tweets=max_tweets)
        tweets = result.get("tweets", [])

        stats = await save_tweets(db, tweets, search_query=query)

        await db.execute(
            update(SyncRun).where(SyncRun.id == run.id).values(
                completed_at=datetime.utcnow(), status="completed",
                records_fetched=len(tweets), records_new=stats["new"],
                records_error=stats["error"],
            )
        )
        await db.commit()
        logger.info(f"Twitter search '{query}': {stats}")

    except Exception as e:
        logger.error(f"Twitter search failed: {e}")
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
        await scraper.cleanup()
        await db.close()


async def scrape_user(settings: Settings, username: str, count: int | None = None, headless: bool | None = None):
    """Scrape a user's timeline and save. Called from route background task."""
    db = await get_session()
    scraper = _create_scraper(settings, headless)

    try:
        if not await scraper.initialize():
            raise RuntimeError("Failed to initialize browser")
        if not await scraper.login():
            raise RuntimeError("Failed to login to Twitter")

        max_tweets = count or settings.twitter_default_count

        run = SyncRun(
            source="twitter", status="running",
            run_metadata={"username": username, "max_tweets": max_tweets},
        )
        db.add(run)
        await db.commit()
        await db.refresh(run)

        result = await scraper.scrape_user_tweets(username=username, max_tweets=max_tweets)
        tweets = result.get("tweets", [])

        stats = await save_tweets(db, tweets, search_query=f"@{username}")

        await db.execute(
            update(SyncRun).where(SyncRun.id == run.id).values(
                completed_at=datetime.utcnow(), status="completed",
                records_fetched=len(tweets), records_new=stats["new"],
                records_error=stats["error"],
            )
        )
        await db.commit()
        logger.info(f"Twitter @{username}: {stats}")

    except Exception as e:
        logger.error(f"Twitter user scrape failed: {e}")
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
        await scraper.cleanup()
        await db.close()
