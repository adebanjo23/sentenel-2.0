"""Playwright-based Twitter/X scraper — browser automation + tweet parsing."""

import asyncio
import json
import logging
import random
import time
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Optional

from playwright.async_api import (
    Browser, BrowserContext, Page, Response,
    async_playwright,
)

try:
    import jmespath
except ImportError:
    jmespath = None

logger = logging.getLogger("sentinel.twitter.scraper")


class PlaywrightScraper:
    """Drives Chromium to scrape Twitter. Intercepts GraphQL API responses."""

    def __init__(
        self,
        username: str,
        password: str,
        email: str,
        headless: bool = True,
        cookies_file: str = "./data/twitter/cookies/playwright_cookies.json",
        scroll_delay_min: float = 3.0,
        scroll_delay_max: float = 6.0,
        max_scroll_attempts: int = 200,
        max_attempts_without_new: int = 15,
        page_load_timeout: int = 60000,
        element_wait_timeout: int = 30000,
    ):
        self.username = username
        self.password = password
        self.email = email
        self.headless = headless
        self.cookies_file = cookies_file
        self.scroll_delay_min = scroll_delay_min
        self.scroll_delay_max = scroll_delay_max
        self.max_scroll_attempts = max_scroll_attempts
        self.max_attempts_without_new = max_attempts_without_new
        self.page_load_timeout = page_load_timeout
        self.element_wait_timeout = element_wait_timeout

        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.is_logged_in = False

        self.scraped_tweet_ids: set[str] = set()
        self.all_tweets: list[dict] = []
        self.user_data: dict | None = None
        self.start_time: float | None = None

    # ------------------------------------------------------------------
    # Browser setup
    # ------------------------------------------------------------------

    async def initialize(self) -> bool:
        try:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=self.headless,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                ],
            )
            self.context = await self.browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                ),
                locale="en-US",
                timezone_id="America/New_York",
            )

            # Load saved cookies
            cookies_path = Path(self.cookies_file)
            if cookies_path.exists():
                try:
                    cookies_data = json.loads(cookies_path.read_text())
                    if cookies_data:
                        await self.context.add_cookies(cookies_data)
                        self.is_logged_in = True
                        logger.info("Loaded saved cookies")
                except Exception as e:
                    logger.warning(f"Failed to load cookies: {e}")

            self.page = await self.context.new_page()
            self.page.on("response", self._intercept_response)
            logger.info("Browser initialized")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize browser: {e}")
            return False

    # ------------------------------------------------------------------
    # Response interception — captures GraphQL tweet data
    # ------------------------------------------------------------------

    async def _intercept_response(self, response: Response):
        try:
            if response.request.resource_type not in ("xhr", "fetch"):
                return
            url = response.url
            endpoints = [
                "UserByScreenName", "UserTweets", "TweetDetail",
                "TweetResultByRestId", "SearchTimeline", "SearchAdaptive",
            ]
            if not any(ep in url for ep in endpoints):
                return

            data = await response.json()

            # Save one raw response for debugging field mapping
            if not hasattr(self, '_debug_saved') or not self._debug_saved:
                try:
                    debug_path = Path("data/twitter/debug_graphql_response.json")
                    debug_path.parent.mkdir(parents=True, exist_ok=True)
                    debug_path.write_text(json.dumps(data, indent=2, default=str)[:50000])
                    self._debug_saved = True
                    logger.info(f"Saved debug GraphQL response to {debug_path}")
                except Exception:
                    pass

            if "UserByScreenName" in url:
                self._parse_user_data(data)
            elif "UserTweets" in url or "SearchTimeline" in url or "SearchAdaptive" in url:
                self._parse_tweets_from_timeline(data)
            elif "TweetDetail" in url or "TweetResultByRestId" in url:
                self._parse_single_tweet(data)

        except Exception:
            pass

    # ------------------------------------------------------------------
    # Tweet parsing
    # ------------------------------------------------------------------

    def _parse_user_data(self, data: dict):
        if not jmespath:
            return
        try:
            user_result = jmespath.search("data.user.result", data)
            if not user_result:
                return
            legacy = user_result.get("legacy", {})
            self.user_data = {
                "id": user_result.get("rest_id", ""),
                "username": legacy.get("screen_name", ""),
                "display_name": legacy.get("name", ""),
                "followers_count": legacy.get("followers_count", 0),
                "verified": user_result.get("is_blue_verified", False) or legacy.get("verified", False),
            }
            logger.info(f"Captured user: @{self.user_data['username']} ({self.user_data['followers_count']} followers)")
        except Exception as e:
            logger.error(f"Error parsing user data: {e}")

    def _parse_tweets_from_timeline(self, data: dict):
        if not jmespath:
            return
        try:
            instructions = (
                jmespath.search("data.user.result.timeline_v2.timeline.instructions", data)
                or jmespath.search("data.user.result.timeline.timeline.instructions", data)
                or jmespath.search("data.search_by_raw_query.search_timeline.timeline.instructions", data)
                or jmespath.search("data.threaded_conversation_with_injections_v2.instructions", data)
            )
            if not instructions:
                return

            for instruction in instructions:
                if instruction.get("type") != "TimelineAddEntries":
                    continue
                for entry in instruction.get("entries", []):
                    entry_id = entry.get("entryId", "")
                    if any(skip in entry_id for skip in ["cursor-", "who-to-follow", "profile-conversation"]):
                        continue
                    tweet_result = jmespath.search("content.itemContent.tweet_results.result", entry)
                    if tweet_result:
                        parsed = self._extract_tweet_data(tweet_result)
                        if parsed and parsed.get("id") and parsed["id"] not in self.scraped_tweet_ids:
                            self.all_tweets.append(parsed)
                            self.scraped_tweet_ids.add(parsed["id"])

        except Exception as e:
            logger.error(f"Error parsing timeline: {e}")

    def _parse_single_tweet(self, data: dict):
        if not jmespath:
            return
        try:
            tweet_result = jmespath.search("data.tweetResult.result", data)
            if tweet_result:
                parsed = self._extract_tweet_data(tweet_result)
                if parsed and parsed.get("id") and parsed["id"] not in self.scraped_tweet_ids:
                    self.all_tweets.append(parsed)
                    self.scraped_tweet_ids.add(parsed["id"])
        except Exception as e:
            logger.error(f"Error parsing single tweet: {e}")

    def _extract_tweet_data(self, tweet_result: dict) -> dict | None:
        try:
            if tweet_result.get("__typename") == "TweetWithVisibilityResults":
                tweet_result = tweet_result.get("tweet", {})

            legacy = tweet_result.get("legacy", {})
            tweet_id = tweet_result.get("rest_id", "")
            user_result = tweet_result.get("core", {}).get("user_results", {}).get("result", {})
            user_legacy = user_result.get("legacy", {})

            # Debug: log user structure if screen_name is missing
            if not user_legacy.get("screen_name"):
                logger.debug(f"user_result keys: {list(user_result.keys())}")
                logger.debug(f"user_legacy keys: {list(user_legacy.keys())}")
                # Try alternative paths Twitter may have moved user data to
                if not user_legacy and "legacy" not in user_result:
                    # Some responses nest user under different paths
                    for key in ["user", "user_result", "author"]:
                        alt = tweet_result.get(key, {})
                        if alt and alt.get("screen_name"):
                            user_legacy = alt
                            break
                        if alt and alt.get("legacy", {}).get("screen_name"):
                            user_legacy = alt.get("legacy", {})
                            user_result = alt
                            break

            # Media
            media = []
            for item in legacy.get("extended_entities", {}).get("media", []):
                info = {"type": item.get("type", ""), "url": item.get("media_url_https", "")}
                if item.get("type") == "video":
                    variants = [v for v in item.get("video_info", {}).get("variants", [])
                                if v.get("content_type") == "video/mp4"]
                    if variants:
                        info["video_url"] = max(variants, key=lambda v: v.get("bitrate", 0))["url"]
                media.append(info)

            hashtags = [ht.get("text", "") for ht in legacy.get("entities", {}).get("hashtags", [])]

            return {
                "id": tweet_id,
                "text": legacy.get("full_text", ""),
                "full_text": legacy.get("full_text", ""),
                "created_at": legacy.get("created_at", ""),
                "user": {
                    "id": user_result.get("rest_id", ""),
                    "username": user_legacy.get("screen_name", ""),
                    "display_name": user_legacy.get("name", ""),
                    "followers_count": user_legacy.get("followers_count", 0),
                    "verified": user_result.get("is_blue_verified", False) or user_legacy.get("verified", False),
                },
                "metrics": {
                    "retweet_count": legacy.get("retweet_count", 0),
                    "favorite_count": legacy.get("favorite_count", 0),
                    "reply_count": legacy.get("reply_count", 0),
                    "quote_count": legacy.get("quote_count", 0),
                    "view_count": tweet_result.get("views", {}).get("count", 0),
                },
                "lang": legacy.get("lang", "en"),
                "is_retweet": legacy.get("retweeted", False),
                "is_reply": legacy.get("in_reply_to_status_id_str") is not None,
                "hashtags": hashtags,
                "media": media,
                "scraped_at": time.time(),
            }
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Login
    # ------------------------------------------------------------------

    async def login(self) -> bool:
        if not self.page or not self.context:
            raise RuntimeError("Browser not initialized")

        # Try cookies first
        if self.is_logged_in:
            try:
                await self.page.goto("https://x.com/home", wait_until="domcontentloaded", timeout=self.element_wait_timeout)
                try:
                    await self.page.wait_for_selector('[data-testid="SideNav_NewTweet_Button"]', timeout=10000)
                    logger.info("Cookie login verified")
                    return True
                except Exception:
                    if "login" not in self.page.url and "flow" not in self.page.url:
                        return True
                    self.is_logged_in = False
            except Exception:
                self.is_logged_in = False

        logger.info("Logging in to Twitter...")
        try:
            await self.page.goto("https://twitter.com/i/flow/login", wait_until="domcontentloaded", timeout=self.page_load_timeout)
            await asyncio.sleep(5)

            # Username — try multiple selectors (Twitter changes these)
            logger.info("Waiting for username field...")
            username_input = None
            for selector in [
                'input[autocomplete="username"]',
                'input[name="text"]',
                'input[type="text"]',
            ]:
                try:
                    username_input = await self.page.wait_for_selector(selector, timeout=10000)
                    if username_input:
                        logger.info(f"Found username field with selector: {selector}")
                        break
                except Exception:
                    continue
            if not username_input:
                screenshot_path = Path("data/twitter/login_no_username_field.png")
                screenshot_path.parent.mkdir(parents=True, exist_ok=True)
                await self.page.screenshot(path=str(screenshot_path))
                logger.error(f"Could not find username field. Screenshot: {screenshot_path}")
                return False
            logger.info(f"Entering username: {self.username}")
            await username_input.fill(self.username)
            await asyncio.sleep(1)
            next_btn = await self.page.wait_for_selector('button:has-text("Next")', timeout=10000)
            await next_btn.click()
            logger.info("Clicked Next, waiting for next step...")
            await asyncio.sleep(3)

            # Email/phone verification (Twitter sometimes asks this)
            try:
                email_input = await self.page.wait_for_selector('input[data-testid="ocfEnterTextTextInput"]', timeout=8000)
                if email_input:
                    logger.info("Email/phone verification required, entering email...")
                    await email_input.fill(self.email)
                    await asyncio.sleep(1)
                    next_btn = await self.page.wait_for_selector('button:has-text("Next")')
                    await next_btn.click()
                    logger.info("Clicked Next after email verification")
                    await asyncio.sleep(3)
            except Exception:
                logger.info("No email verification needed")

            # Password
            logger.info("Waiting for password field...")
            try:
                password_input = await self.page.wait_for_selector('input[name="password"]', timeout=self.element_wait_timeout)
            except Exception:
                # Take screenshot to see what Twitter is showing
                screenshot_path = Path("data/twitter/login_stuck.png")
                screenshot_path.parent.mkdir(parents=True, exist_ok=True)
                await self.page.screenshot(path=str(screenshot_path))
                logger.error(f"Password field not found. Screenshot saved to {screenshot_path}")
                logger.error(f"Current URL: {self.page.url}")
                logger.error("Twitter may be showing a CAPTCHA or other challenge. Check the screenshot.")
                return False

            logger.info("Entering password...")
            await password_input.fill(self.password)
            await asyncio.sleep(1)
            login_btn = await self.page.wait_for_selector('button[data-testid="LoginForm_Login_Button"]', timeout=10000)
            await login_btn.click()
            logger.info("Clicked Login, waiting for redirect...")
            await asyncio.sleep(5)

            # Verify login
            try:
                await self.page.wait_for_url("https://twitter.com/home", timeout=15000)
            except Exception:
                current_url = self.page.url
                # x.com also counts as logged in
                if any(ok in current_url for ok in ["x.com/home", "twitter.com/home"]):
                    pass
                elif "login" in current_url or "flow" in current_url:
                    screenshot_path = Path("data/twitter/login_failed.png")
                    screenshot_path.parent.mkdir(parents=True, exist_ok=True)
                    await self.page.screenshot(path=str(screenshot_path))
                    logger.error(f"Login failed — stuck on: {current_url}. Screenshot: {screenshot_path}")
                    return False

            self.is_logged_in = True
            # Save cookies
            cookies_path = Path(self.cookies_file)
            cookies_path.parent.mkdir(parents=True, exist_ok=True)
            cookies = await self.context.cookies()
            cookies_path.write_text(json.dumps(cookies, indent=2))
            logger.info("Login successful, cookies saved")
            return True

        except Exception as e:
            # Screenshot on any unexpected error
            try:
                screenshot_path = Path("data/twitter/login_error.png")
                screenshot_path.parent.mkdir(parents=True, exist_ok=True)
                await self.page.screenshot(path=str(screenshot_path))
                logger.error(f"Login failed: {e}. Screenshot: {screenshot_path}")
            except Exception:
                logger.error(f"Login failed: {e}")
            return False

    # ------------------------------------------------------------------
    # Scraping
    # ------------------------------------------------------------------

    async def search_tweets(self, query: str, max_tweets: int | None = None) -> dict:
        """Search Twitter for a query."""
        if not self.page:
            raise RuntimeError("Browser not initialized")

        self._reset(max_tweets)
        encoded = urllib.parse.quote(query)
        url = f"https://twitter.com/search?q={encoded}&src=typed_query&f=live"

        logger.info(f"Searching: '{query}' (limit: {max_tweets})")
        try:
            await self.page.goto(url, wait_until="domcontentloaded", timeout=self.page_load_timeout)
            await asyncio.sleep(3)

            try:
                await self.page.wait_for_selector('[data-testid="tweet"]', timeout=self.element_wait_timeout)
            except Exception:
                return {"query": query, "tweets": [], "tweet_count": 0}

            await self._scroll_timeline(max_tweets)
            logger.info(f"Search done: {len(self.all_tweets)} tweets for '{query}'")
            return {"query": query, "tweets": self.all_tweets, "tweet_count": len(self.all_tweets)}

        except Exception as e:
            logger.error(f"Search failed: {e}")
            return {"error": str(e), "query": query, "tweets": self.all_tweets}

    async def _detect_rate_limit(self) -> bool:
        """Check if Twitter is showing a rate limit or error page."""
        if not self.page:
            return False
        try:
            # Check for common rate limit / error indicators
            page_text = await self.page.text_content("body") or ""
            rate_limit_signals = [
                "Rate limit exceeded",
                "Something went wrong. Try reloading",
                "Something went wrong, but don",
                "Hmm...this page doesn",
            ]
            for signal in rate_limit_signals:
                if signal.lower() in page_text.lower():
                    return True

            # Check if redirected to a login/captcha page
            url = self.page.url
            if any(x in url for x in ["/account/access", "challenge", "captcha"]):
                return True

            return False
        except Exception:
            return False

    async def scrape_user_tweets(self, username: str, max_tweets: int | None = None) -> dict:
        """Scrape a user's timeline."""
        if not self.page:
            raise RuntimeError("Browser not initialized")

        self._reset(max_tweets)
        logger.info(f"Scraping @{username} (limit: {max_tweets})")

        try:
            await self.page.goto(f"https://twitter.com/{username}", wait_until="domcontentloaded", timeout=self.page_load_timeout)
            await asyncio.sleep(3)

            # Detect rate limit before trying to scrape
            if await self._detect_rate_limit():
                logger.warning(f"Rate limited before scraping @{username}")
                return {"error": "rate_limited", "rate_limited": True, "tweets": []}

            try:
                await self.page.wait_for_selector('[data-testid="tweet"]', timeout=self.element_wait_timeout)
            except Exception:
                # No tweets loaded — could be rate limit or empty profile
                if await self._detect_rate_limit():
                    logger.warning(f"Rate limited while loading @{username}")
                    return {"error": "rate_limited", "rate_limited": True, "tweets": []}

            await self._scroll_timeline(max_tweets)

            # Check if we got suspiciously few tweets (possible silent rate limit)
            if max_tweets and max_tweets > 20 and len(self.all_tweets) == 0:
                if await self._detect_rate_limit():
                    logger.warning(f"Rate limited during scroll for @{username}")
                    return {"error": "rate_limited", "rate_limited": True, "tweets": []}

            elapsed = time.time() - (self.start_time or time.time())
            logger.info(f"Scraped {len(self.all_tweets)} tweets from @{username} in {elapsed:.1f}s")
            return {
                "username": username,
                "user_data": self.user_data,
                "tweets": self.all_tweets,
                "tweet_count": len(self.all_tweets),
                "rate_limited": False,
            }

        except Exception as e:
            logger.error(f"User scrape failed: {e}")
            return {"error": str(e), "tweets": self.all_tweets}

    # ------------------------------------------------------------------
    # Scrolling
    # ------------------------------------------------------------------

    def _reset(self, max_tweets: int | None = None):
        self.scraped_tweet_ids.clear()
        self.all_tweets.clear()
        self.user_data = None
        self.start_time = time.time()
        self._max_tweets = max_tweets

    async def _scroll_timeline(self, max_tweets: int | None = None):
        if not self.page:
            return

        scroll_attempts = 0
        no_new_count = 0

        while scroll_attempts < self.max_scroll_attempts:
            scroll_attempts += 1
            before = len(self.all_tweets)

            await self.page.evaluate("window.scrollBy(0, window.innerHeight * 0.8)")
            await asyncio.sleep(random.uniform(self.scroll_delay_min, self.scroll_delay_max))

            new_count = len(self.all_tweets) - before

            if new_count > 0:
                no_new_count = 0
                logger.debug(f"Scroll {scroll_attempts}: +{new_count} (total: {len(self.all_tweets)})")
            else:
                no_new_count += 1
                if no_new_count >= self.max_attempts_without_new:
                    logger.info(f"No new tweets for {self.max_attempts_without_new} scrolls — stopping")
                    break

            if max_tweets and len(self.all_tweets) >= max_tweets:
                logger.info(f"Reached limit: {len(self.all_tweets)}/{max_tweets}")
                break

            # Check if at bottom
            at_bottom = await self.page.evaluate(
                "() => window.innerHeight + window.scrollY >= document.body.scrollHeight - 100"
            )
            if at_bottom and no_new_count > 5:
                logger.info("Reached bottom of timeline")
                break

        logger.info(f"Scrolling done: {scroll_attempts} attempts, {len(self.all_tweets)} tweets")

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    async def cleanup(self):
        try:
            if self.page:
                await self.page.close()
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
            logger.info("Browser cleaned up")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
