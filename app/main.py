import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.database import init_db
from app.routes import health, acled, firms, tiktok, twitter, monitor, events, pipeline, threats, alerts
from app.routes import scheduler as scheduler_route
from app.routes import replay


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Configure root logging — console output for all loggers
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    settings = get_settings()

    # Initialize CloudWatch logger (adds CloudWatch handler in production, no-op in development)
    # Only adds to the "sentinel" logger — basicConfig handles console for everything
    from app.utils.cloudwatch_logger import CloudWatchLogger
    CloudWatchLogger()
    logging.getLogger("sentinel").info(f"SENTINEL 2.0 starting — environment: {settings.cloudwatch_environment}")

    await init_db(settings.database_url)

    # Auto-start scheduler if enabled
    if settings.scheduler_enabled:
        from app.services.scheduler import scheduler
        scheduler.start(settings)

    yield

    # Shutdown — stop scheduler
    from app.services.scheduler import scheduler
    if scheduler.running:
        scheduler.stop()


app = FastAPI(title="SENTINEL 2.0", version="2.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=get_settings().cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Data collection
app.include_router(health.router, prefix="/api", tags=["health"])
app.include_router(acled.router, prefix="/api/acled", tags=["acled"])
app.include_router(firms.router, prefix="/api/firms", tags=["firms"])
app.include_router(tiktok.router, prefix="/api/tiktok", tags=["tiktok"])
app.include_router(twitter.router, prefix="/api/twitter", tags=["twitter"])

# Intelligence
app.include_router(monitor.router, prefix="/api/monitor", tags=["monitor"])
app.include_router(events.router, prefix="/api/events", tags=["events"])

# Pipeline & Threat Assessment
app.include_router(pipeline.router, prefix="/api/pipeline", tags=["pipeline"])
app.include_router(threats.router, prefix="/api/threats", tags=["threats"])
app.include_router(alerts.router, prefix="/api/alerts", tags=["alerts"])

# Scheduler
app.include_router(scheduler_route.router, prefix="/api/scheduler", tags=["scheduler"])

# Historical Replay
app.include_router(replay.router, prefix="/api/replay", tags=["replay"])
