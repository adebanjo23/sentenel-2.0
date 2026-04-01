"""TikTok service — scrape videos by keyword/hashtag/watchlist + store in DB."""

import asyncio
import base64
import logging
import os
import random
import time
from datetime import datetime
from pathlib import Path

from sqlalchemy import select, update, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.database import get_session
from app.exceptions import ScraperError
from app.models import TikTokVideo, SyncRun

logger = logging.getLogger("sentinel.tiktok")

VIDEO_DIR = "./data/videos/tiktok"

# JavaScript that fetches a URL as binary via the browser session.
FETCH_VIDEO_JS = '''async (url) => {
    const resp = await fetch(url, { credentials: 'include' });
    if (!resp.ok) return null;
    const buf = await resp.arrayBuffer();
    const bytes = new Uint8Array(buf);
    let binary = '';
    const chunk = 8192;
    for (let i = 0; i < bytes.length; i += chunk) {
        binary += String.fromCharCode.apply(null, bytes.subarray(i, i + chunk));
    }
    return btoa(binary);
}'''


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------

class TikTokSession:
    """Manages TikTokApi session creation with retry."""

    def __init__(self, ms_token: str):
        self.ms_token = ms_token
        self.api = None
        self.is_active = False

    async def create(self, max_retries: int = 3):
        from TikTokApi import TikTokApi

        if self.api:
            await self.cleanup()

        last_error = None
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Creating TikTok session (attempt {attempt}/{max_retries})...")
                self.api = TikTokApi()
                await self.api.create_sessions(
                    ms_tokens=[self.ms_token],
                    num_sessions=1,
                    sleep_after=3,
                    headless=True,
                    browser="chromium",
                    timeout=60000,
                    suppress_resource_load_types=["image", "media", "font", "stylesheet"],
                    enable_session_recovery=True,
                )
                self.is_active = True
                logger.info(f"TikTok session created on attempt {attempt}")
                return self.api
            except Exception as e:
                last_error = e
                logger.warning(f"Session attempt {attempt}/{max_retries} failed: {e}")
                if self.api:
                    try:
                        await self.api.close_sessions()
                    except Exception:
                        pass
                    try:
                        await self.api.stop_playwright()
                    except Exception:
                        pass
                    self.api = None
                if attempt < max_retries:
                    await asyncio.sleep(5 * attempt)

        self.is_active = False
        raise ScraperError(f"Failed to create TikTok session after {max_retries} attempts: {last_error}")

    async def ensure_valid(self):
        if self.api and self.is_active:
            try:
                health = await self.api.health_check()
                if health.get("healthy_sessions", 0) > 0:
                    return self.api
            except Exception:
                pass
        await self.cleanup()
        return await self.create()

    async def cleanup(self):
        if self.api:
            try:
                await self.api.close_sessions()
            except Exception:
                pass
            try:
                await self.api.stop_playwright()
            except Exception:
                pass
            self.api = None
            self.is_active = False


# ---------------------------------------------------------------------------
# Video downloading
# ---------------------------------------------------------------------------

def _get_download_url(video_data: dict) -> str | None:
    video_meta = video_data.get("video", {})
    return video_meta.get("downloadAddr") or video_meta.get("playAddr")


async def _fetch_via_browser(url: str, api) -> bytes | None:
    try:
        _, session = api._get_session()
        b64_data = await session.page.evaluate(FETCH_VIDEO_JS, url)
        if not b64_data:
            return None
        return base64.b64decode(b64_data)
    except Exception as e:
        logger.error(f"Browser fetch failed: {e}")
        return None


async def download_video(video_data: dict, api, video_id: str) -> str | None:
    """Download video to local disk. Returns file path or None."""
    today = datetime.now().strftime("%Y-%m-%d")
    dest_dir = Path(VIDEO_DIR) / today
    dest_path = dest_dir / f"{video_id}.mp4"

    if dest_path.exists() and dest_path.stat().st_size > 1000:
        return str(dest_path)

    url = _get_download_url(video_data)
    if not url:
        logger.warning(f"No download URL for video {video_id}")
        return None

    try:
        video_bytes = await _fetch_via_browser(url, api)
        if not video_bytes or len(video_bytes) < 1000:
            logger.warning(f"Video {video_id}: got {len(video_bytes) if video_bytes else 0} bytes (likely blocked)")
            return None

        dest_dir.mkdir(parents=True, exist_ok=True)
        with open(dest_path, "wb") as f:
            f.write(video_bytes)

        size_mb = len(video_bytes) / (1024 * 1024)
        logger.info(f"Downloaded {video_id} ({size_mb:.1f} MB)")
        return str(dest_path)

    except Exception as e:
        logger.error(f"Download failed for {video_id}: {e}")
        if dest_path.exists():
            dest_path.unlink()
        return None


# ---------------------------------------------------------------------------
# Storage functions
# ---------------------------------------------------------------------------

async def save_video(db: AsyncSession, video_data: dict, source_type: str, source_query: str) -> bool:
    """Save video metadata. Returns True if new."""
    try:
        video_id = video_data.get("id")
        if not video_id:
            return False

        existing = await db.execute(
            select(TikTokVideo).where(TikTokVideo.video_id == str(video_id))
        )
        if existing.scalar_one_or_none() is not None:
            return False

        create_time = video_data.get("createTime")
        posted_at = datetime.fromtimestamp(create_time) if create_time else None

        video = TikTokVideo(
            video_id=str(video_id),
            author=video_data.get("author", {}).get("uniqueId"),
            author_id=video_data.get("author", {}).get("id"),
            description=video_data.get("desc"),
            hashtags=",".join(tag.get("name", "") for tag in video_data.get("challenges", [])),
            sound_id=video_data.get("music", {}).get("id"),
            sound_title=video_data.get("music", {}).get("title"),
            posted_at=posted_at,
            collected_at=datetime.utcnow(),
            source_type=source_type,
            source_query=source_query,
            likes=video_data.get("stats", {}).get("diggCount", 0),
            comments=video_data.get("stats", {}).get("commentCount", 0),
            shares=video_data.get("stats", {}).get("shareCount", 0),
            views=video_data.get("stats", {}).get("playCount", 0),
            duration=video_data.get("video", {}).get("duration", 0),
            video_downloaded=False,
            raw_json=video_data,
        )
        db.add(video)
        await db.commit()
        return True

    except Exception as e:
        logger.error(f"Error saving video {video_data.get('id')}: {e}")
        await db.rollback()
        return False


async def mark_downloaded(db: AsyncSession, video_id: str, path: str):
    try:
        await db.execute(
            update(TikTokVideo).where(TikTokVideo.video_id == video_id)
            .values(video_url=path, video_downloaded=True)
        )
        await db.commit()
    except Exception as e:
        logger.error(f"Error marking {video_id} downloaded: {e}")
        await db.rollback()


async def is_duplicate(db: AsyncSession, video_id: str) -> bool:
    result = await db.execute(
        select(TikTokVideo).where(TikTokVideo.video_id == str(video_id))
    )
    return result.scalar_one_or_none() is not None


async def get_stats(db: AsyncSession) -> dict:
    total = (await db.execute(select(func.count(TikTokVideo.id)))).scalar()
    downloaded = (await db.execute(
        select(func.count(TikTokVideo.id)).where(TikTokVideo.video_downloaded == True)
    )).scalar()
    by_source = {
        r.source_type: r.count
        for r in (await db.execute(
            select(TikTokVideo.source_type, func.count(TikTokVideo.id).label("count"))
            .group_by(TikTokVideo.source_type).order_by(desc("count"))
        )).all()
    }
    return {"total_videos": total, "downloaded": downloaded, "by_source_type": by_source}


# ---------------------------------------------------------------------------
# Core scraping
# ---------------------------------------------------------------------------

async def search_keywords(
    settings: Settings, db: AsyncSession, api, keywords: list[str], max_per: int,
) -> dict:
    """Search TikTok by keywords."""
    stats = {"keywords_processed": 0, "new_videos": 0, "downloaded": 0, "errors": 0}

    for keyword in keywords:
        logger.info(f"Searching keyword: '{keyword}'")
        try:
            cursor = 0
            found = 0
            while found < max_per:
                response = await api.make_request(
                    url="https://www.tiktok.com/api/search/item/full/",
                    params={"keyword": keyword, "count": 10, "cursor": cursor, "source": "search_video"},
                )
                if response is None:
                    break

                items = response.get("item_list", [])
                if not items:
                    break

                for video_data in items:
                    video_id = video_data.get("id")
                    if not video_id:
                        continue
                    found += 1

                    if await is_duplicate(db, video_id):
                        continue

                    await save_video(db, video_data, "keyword", keyword)
                    stats["new_videos"] += 1
                    logger.info(f"  New: {video_id} by @{video_data.get('author', {}).get('uniqueId', '?')}")

                    if settings.tiktok_download_videos:
                        path = await download_video(video_data, api, str(video_id))
                        if path:
                            await mark_downloaded(db, str(video_id), path)
                            stats["downloaded"] += 1

                if not response.get("has_more", False):
                    break
                cursor = response.get("cursor", 0)
                await asyncio.sleep(random.uniform(settings.tiktok_rate_delay_min, settings.tiktok_rate_delay_max))

        except Exception as e:
            logger.error(f"Keyword search failed for '{keyword}': {e}")
            stats["errors"] += 1

        stats["keywords_processed"] += 1
        await asyncio.sleep(5)

    return stats


async def search_hashtags(
    settings: Settings, db: AsyncSession, api, hashtags: list[str], max_per: int,
) -> dict:
    """Search TikTok by hashtags."""
    stats = {"hashtags_processed": 0, "new_videos": 0, "downloaded": 0, "errors": 0}

    for tag_name in hashtags:
        tag_name = tag_name.lstrip("#")
        logger.info(f"Searching hashtag: #{tag_name}")
        try:
            hashtag = api.hashtag(name=tag_name)
            count = 0
            async for video in hashtag.videos(count=max_per):
                video_id = video.id
                count += 1

                if await is_duplicate(db, video_id):
                    continue

                await save_video(db, video.as_dict, "hashtag", tag_name)
                stats["new_videos"] += 1
                logger.info(f"  New: {video_id} by @{getattr(video.author, 'username', '?')}")

                if settings.tiktok_download_videos:
                    # For hashtag videos, get download URL from as_dict
                    url = _get_download_url(video.as_dict)
                    if url:
                        path = await download_video(video.as_dict, api, str(video_id))
                        if path:
                            await mark_downloaded(db, str(video_id), path)
                            stats["downloaded"] += 1

                if count % 5 == 0:
                    await asyncio.sleep(random.uniform(settings.tiktok_rate_delay_min, settings.tiktok_rate_delay_max))

        except Exception as e:
            logger.error(f"Hashtag search failed for #{tag_name}: {e}")
            stats["errors"] += 1

        stats["hashtags_processed"] += 1
        await asyncio.sleep(5)

    return stats


async def monitor_users(
    settings: Settings, db: AsyncSession, api, usernames: list[str], max_per: int,
) -> dict:
    """Monitor watchlist accounts."""
    stats = {"accounts_processed": 0, "new_videos": 0, "downloaded": 0, "errors": 0}

    for username in usernames:
        logger.info(f"Monitoring user: @{username}")
        try:
            user = api.user(username=username)
            count = 0
            async for video in user.videos(count=max_per):
                video_id = video.id
                count += 1

                if await is_duplicate(db, video_id):
                    continue

                await save_video(db, video.as_dict, "watchlist", username)
                stats["new_videos"] += 1

                if settings.tiktok_download_videos:
                    path = await download_video(video.as_dict, api, str(video_id))
                    if path:
                        await mark_downloaded(db, str(video_id), path)
                        stats["downloaded"] += 1

                if count % 5 == 0:
                    await asyncio.sleep(random.uniform(settings.tiktok_rate_delay_min, settings.tiktok_rate_delay_max))

        except Exception as e:
            logger.error(f"User monitor failed for @{username}: {e}")
            stats["errors"] += 1

        stats["accounts_processed"] += 1
        await asyncio.sleep(5)

    return stats


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

async def run_scrape(
    settings: Settings,
    keywords: list[str] | None = None,
    hashtags: list[str] | None = None,
    users: list[str] | None = None,
    max_per_keyword: int | None = None,
    max_per_hashtag: int | None = None,
    max_per_user: int | None = None,
):
    """Full scrape cycle. Called from route background task."""
    db = await get_session()
    session = TikTokSession(settings.tiktok_ms_token)

    try:
        api = await session.create()
        cycle_start = time.time()

        # Defaults from settings
        kw = keywords or [k.strip() for k in settings.tiktok_keywords_en.split(",")]
        kw += [k.strip() for k in settings.tiktok_keywords_ha.split(",") if k.strip()]
        ht = hashtags or [h.strip() for h in settings.tiktok_hashtags.split(",") if h.strip()]
        wl = users or [u.strip() for u in settings.tiktok_watchlist.split(",") if u.strip()]

        results = {}
        if kw:
            results["keywords"] = await search_keywords(
                settings, db, api, kw, max_per_keyword or settings.tiktok_max_per_keyword,
            )
        if ht:
            results["hashtags"] = await search_hashtags(
                settings, db, api, ht, max_per_hashtag or settings.tiktok_max_per_hashtag,
            )
        if wl:
            results["watchlist"] = await monitor_users(
                settings, db, api, wl, max_per_user or settings.tiktok_max_per_user,
            )

        duration = time.time() - cycle_start
        total_new = sum(r.get("new_videos", 0) for r in results.values() if isinstance(r, dict))
        logger.info(f"TikTok cycle complete in {duration:.1f}s — {total_new} new videos")

    except Exception as e:
        logger.error(f"TikTok scrape failed: {e}")
    finally:
        await session.cleanup()
        await db.close()
