"""Monitor service — scrape watchlist accounts for recent tweets."""

import asyncio
import json
import logging
from pathlib import Path

from app.config import Settings
from app.database import get_session
from app.services.twitter_scraper import PlaywrightScraper
from app.services.twitter_service import save_tweets

logger = logging.getLogger("sentinel.monitor")

WATCHLIST_PATH = "./data/twitter/nigeria_security_watchlist.json"

# Rate limit settings
RATE_LIMIT_INITIAL_WAIT = 120      # 2 minutes on first rate limit
RATE_LIMIT_MAX_WAIT = 600          # 10 minutes max
RATE_LIMIT_MAX_RETRIES = 3         # retry each account up to 3 times
PAUSE_BETWEEN_ACCOUNTS = 5         # seconds between accounts (avoid triggering limits)


def load_watchlist(tier: str | None = None) -> list[dict]:
    """Load watchlist accounts from JSON. Optionally filter by tier."""
    path = Path(WATCHLIST_PATH)
    if not path.exists():
        logger.error(f"Watchlist not found: {WATCHLIST_PATH}")
        return []

    data = json.loads(path.read_text())
    accounts = []
    for category in data.get("categories", []):
        for account in category.get("accounts", []):
            account["category"] = category.get("category", "")
            accounts.append(account)

    if tier:
        tier_map = {
            "1": ["NIGERIAN DEFENSE/MILITARY REPORTERS", "SECURITY-FOCUSED"],
            "2": ["NEWS ORGANIZATIONS"],
            "3": ["MILITARY/POLICE OFFICIAL", "STATE GOVERNMENT"],
            "4": ["INTERNATIONAL", "ANALYSTS", "LOCAL JOURNALISTS"],
        }
        keywords = tier_map.get(tier, [])
        if keywords:
            accounts = [
                a for a in accounts
                if any(k in a["category"].upper() for k in keywords)
            ]

    return accounts


async def scrape_account(scraper: PlaywrightScraper, handle: str, max_tweets: int = 20) -> dict:
    """Scrape recent tweets from a single account. Returns full result dict."""
    username = handle.lstrip("@")
    logger.info(f"Monitoring @{username}...")
    result = await scraper.scrape_user_tweets(username=username, max_tweets=max_tweets)
    tweets = result.get("tweets", [])
    logger.info(f"  @{username}: {len(tweets)} tweets")
    return result


async def run_monitoring_cycle(
    settings: Settings,
    tier: str | None = None,
    accounts: list[str] | None = None,
    max_tweets_per_account: int = 20,
    headless: bool | None = None,
):
    """
    Run one monitoring cycle: scrape watchlist accounts, save tweets.

    Handles Twitter rate limits with exponential backoff and retries.
    Failed accounts are retried after a cooldown period.

    Args:
        tier: "1", "2", "3", "4" — which priority tier to monitor
        accounts: explicit list of @handles (overrides tier/watchlist)
        max_tweets_per_account: how many recent tweets to grab per account
    """
    db = await get_session()

    scraper = PlaywrightScraper(
        username=settings.twitter_username,
        password=settings.twitter_password,
        email=settings.twitter_email,
        headless=headless if headless is not None else settings.twitter_headless,
        scroll_delay_min=settings.twitter_scroll_delay_min,
        scroll_delay_max=settings.twitter_scroll_delay_max,
    )

    try:
        if not await scraper.initialize():
            raise RuntimeError("Failed to initialize browser")
        if not await scraper.login():
            raise RuntimeError("Failed to login to Twitter")

        # Determine which accounts to scrape
        if accounts:
            handles = accounts
        else:
            watchlist = load_watchlist(tier)
            handles = [a["handle"] for a in watchlist]

        if not handles:
            logger.warning("No accounts to monitor")
            return {"accounts": 0, "tweets": 0, "new": 0}

        logger.info(f"Monitoring {len(handles)} accounts (max {max_tweets_per_account} tweets each)...")

        total_tweets = 0
        total_new = 0
        completed = []
        failed = []
        rate_limit_wait = RATE_LIMIT_INITIAL_WAIT
        consecutive_rate_limits = 0

        # First pass — scrape all accounts
        retry_queue = []

        for i, handle in enumerate(handles):
            try:
                result = await scrape_account(scraper, handle, max_tweets_per_account)

                if result.get("rate_limited"):
                    consecutive_rate_limits += 1
                    retry_queue.append(handle)

                    # If we get rate limited 3 times in a row, pause hard
                    if consecutive_rate_limits >= 3:
                        logger.warning(
                            f"Rate limited {consecutive_rate_limits}x in a row. "
                            f"Pausing {rate_limit_wait}s before continuing..."
                        )
                        await asyncio.sleep(rate_limit_wait)
                        rate_limit_wait = min(rate_limit_wait * 2, RATE_LIMIT_MAX_WAIT)
                        consecutive_rate_limits = 0

                        # Verify we're still logged in after the pause
                        if not scraper.is_logged_in:
                            logger.warning("Session may have expired, re-logging in...")
                            if not await scraper.login():
                                logger.error("Re-login failed, stopping")
                                break
                    else:
                        # Short pause on isolated rate limit
                        logger.warning(f"Rate limited on {handle}, pausing 60s...")
                        await asyncio.sleep(60)
                    continue

                # Success — reset consecutive rate limit counter
                consecutive_rate_limits = 0
                rate_limit_wait = RATE_LIMIT_INITIAL_WAIT

                tweets = result.get("tweets", [])
                if tweets:
                    stats = await save_tweets(db, tweets, search_query=f"monitor:{handle}")
                    total_tweets += len(tweets)
                    total_new += stats["new"]
                    completed.append(handle)
                else:
                    completed.append(handle)

                # Pause between accounts to stay under the radar
                if i < len(handles) - 1:
                    await asyncio.sleep(PAUSE_BETWEEN_ACCOUNTS)

            except Exception as e:
                logger.error(f"Failed to monitor {handle}: {e}")
                failed.append(handle)
                continue

        # Retry pass — attempt rate-limited accounts after a cooldown
        if retry_queue:
            logger.info(f"Retrying {len(retry_queue)} rate-limited accounts after {RATE_LIMIT_INITIAL_WAIT}s cooldown...")
            await asyncio.sleep(RATE_LIMIT_INITIAL_WAIT)

            for attempt in range(RATE_LIMIT_MAX_RETRIES):
                if not retry_queue:
                    break

                still_failed = []
                logger.info(f"Retry attempt {attempt + 1}/{RATE_LIMIT_MAX_RETRIES} for {len(retry_queue)} accounts...")

                for handle in retry_queue:
                    try:
                        result = await scrape_account(scraper, handle, max_tweets_per_account)

                        if result.get("rate_limited"):
                            still_failed.append(handle)
                            logger.warning(f"Still rate limited on {handle}")
                            await asyncio.sleep(30)
                            continue

                        tweets = result.get("tweets", [])
                        if tweets:
                            stats = await save_tweets(db, tweets, search_query=f"monitor:{handle}")
                            total_tweets += len(tweets)
                            total_new += stats["new"]
                        completed.append(handle)
                        await asyncio.sleep(PAUSE_BETWEEN_ACCOUNTS)

                    except Exception as e:
                        logger.error(f"Retry failed for {handle}: {e}")
                        still_failed.append(handle)

                retry_queue = still_failed

                if retry_queue:
                    wait = RATE_LIMIT_INITIAL_WAIT * (attempt + 2)
                    logger.info(f"{len(retry_queue)} still rate-limited, waiting {wait}s before next retry...")
                    await asyncio.sleep(wait)

            # Anything still in retry_queue is a final failure
            failed.extend(retry_queue)

        logger.info(
            f"Monitoring complete: {len(completed)}/{len(handles)} accounts, "
            f"{total_tweets} tweets, {total_new} new"
        )
        if failed:
            logger.warning(f"Failed accounts ({len(failed)}): {', '.join(failed)}")

        # Trigger intelligence pipeline on new tweets
        if total_new > 0:
            try:
                from app.services.pipeline import run_pipeline
                run_id = await run_pipeline(settings)
                logger.info(f"Pipeline run #{run_id} triggered after monitoring")
            except Exception as e:
                logger.error(f"Pipeline trigger failed: {e}")

        return {
            "accounts": len(handles),
            "completed": len(completed),
            "failed": len(failed),
            "failed_handles": failed,
            "tweets": total_tweets,
            "new": total_new,
        }

    except Exception as e:
        logger.error(f"Monitoring cycle failed: {e}")
        raise
    finally:
        await scraper.cleanup()
        await db.close()
