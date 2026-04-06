"""Scheduler — runs the monitor + pipeline cycle on a recurring interval."""

import asyncio
import logging
from datetime import datetime

from app.config import Settings
from app.services.monitor_service import run_monitoring_cycle

logger = logging.getLogger("sentinel.scheduler")


class Scheduler:
    """Background scheduler that runs collection + pipeline on an interval."""

    def __init__(self):
        self.running = False
        self.paused = False
        self.task: asyncio.Task | None = None
        self.last_run_at: datetime | None = None
        self.next_run_at: datetime | None = None
        self.last_result: dict | None = None
        self.cycle_count = 0
        self.error_count = 0
        self.last_error: str | None = None

    def start(self, settings: Settings):
        """Start the scheduler loop."""
        if self.running:
            logger.warning("Scheduler already running")
            return

        self.running = True
        self.paused = False
        self.task = asyncio.create_task(self._loop(settings))
        logger.info(
            f"Scheduler started: every {settings.scheduler_interval_hours}h, "
            f"{settings.scheduler_tweets_per_account} tweets/account"
        )

    def stop(self):
        """Stop the scheduler."""
        self.running = False
        if self.task and not self.task.done():
            self.task.cancel()
        logger.info("Scheduler stopped")

    def pause(self):
        """Pause — the loop keeps running but skips cycles."""
        self.paused = True
        logger.info("Scheduler paused")

    def resume(self):
        """Resume after pause."""
        self.paused = False
        logger.info("Scheduler resumed")

    def status(self) -> dict:
        return {
            "running": self.running,
            "paused": self.paused,
            "cycle_count": self.cycle_count,
            "error_count": self.error_count,
            "last_run_at": self.last_run_at.isoformat() if self.last_run_at else None,
            "next_run_at": self.next_run_at.isoformat() if self.next_run_at else None,
            "last_result": self.last_result,
            "last_error": self.last_error,
        }

    async def _loop(self, settings: Settings):
        """Main loop — runs a cycle, sleeps, repeats."""
        interval_seconds = settings.scheduler_interval_hours * 3600

        # Run first cycle after a short delay (let the server finish starting)
        await asyncio.sleep(10)

        while self.running:
            if not self.paused:
                await self._run_cycle(settings)

            # Calculate next run time
            self.next_run_at = datetime.utcnow().replace(microsecond=0)
            from datetime import timedelta
            self.next_run_at += timedelta(seconds=interval_seconds)
            logger.info(f"Next cycle at {self.next_run_at.isoformat()}")

            # Sleep in small chunks so we can respond to stop/pause quickly
            for _ in range(interval_seconds // 10):
                if not self.running:
                    return
                await asyncio.sleep(10)

            # Sleep remaining seconds
            remainder = interval_seconds % 10
            if remainder > 0:
                await asyncio.sleep(remainder)

    async def _run_cycle(self, settings: Settings):
        """Run one full cycle: monitor all accounts → pipeline processes new data."""
        self.cycle_count += 1
        self.last_run_at = datetime.utcnow()
        logger.info(f"=== Scheduler cycle #{self.cycle_count} starting ===")

        try:
            result = await run_monitoring_cycle(
                settings,
                max_tweets_per_account=settings.scheduler_tweets_per_account,
            )

            self.last_result = result
            self.last_error = None

            logger.info(
                f"=== Cycle #{self.cycle_count} complete: "
                f"{result.get('completed', 0)}/{result.get('accounts', 0)} accounts, "
                f"{result.get('tweets', 0)} tweets, {result.get('new', 0)} new ==="
            )

        except Exception as e:
            self.error_count += 1
            self.last_error = str(e)
            logger.error(f"=== Cycle #{self.cycle_count} failed: {e} ===")


# Singleton
scheduler = Scheduler()
