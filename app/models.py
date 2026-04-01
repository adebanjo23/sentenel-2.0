"""SQLAlchemy models — SQLite only, no Postgres branches."""

from datetime import datetime

from sqlalchemy import (
    Boolean, Column, Date, Float, Index, Integer,
    String, Text, DateTime, UniqueConstraint, JSON,
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class ACLEDEvent(Base):
    """Armed Conflict Location & Event Data — verified conflict events."""
    __tablename__ = "acled_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(String(50), unique=True, nullable=False, index=True)

    # Temporal
    event_date = Column(Date, nullable=False, index=True)
    year = Column(Integer)
    timestamp = Column(Integer)

    # Classification
    event_type = Column(String(100), nullable=False, index=True)
    sub_event_type = Column(String(100))
    disorder_type = Column(String(100))

    # Actors
    actor1 = Column(Text)
    actor2 = Column(Text)
    interaction = Column(Integer)

    # Geographic
    country = Column(String(100), default="Nigeria")
    admin1 = Column(String(100), index=True)      # State
    admin2 = Column(String(100), index=True)      # LGA
    admin3 = Column(String(100))
    location_name = Column(Text)
    latitude = Column(Float)
    longitude = Column(Float)
    geo_precision = Column(Integer)

    # Details
    source = Column(Text)
    notes = Column(Text)
    fatalities = Column(Integer, default=0, index=True)
    tags = Column(Text)

    # Metadata
    raw_json = Column(JSON)
    ingested_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class FIRMSHotspot(Base):
    """NASA FIRMS thermal hotspots."""
    __tablename__ = "firms_hotspots"

    id = Column(Integer, primary_key=True, autoincrement=True)

    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)

    # Thermal
    brightness = Column(Float)
    scan = Column(Float)
    track = Column(Float)
    frp = Column(Float)

    # Temporal
    acq_date = Column(Date, nullable=False, index=True)
    acq_time = Column(String(10))
    daynight = Column(String(1))

    # Sensor
    satellite = Column(String(50))
    instrument = Column(String(50))
    source_sensor = Column(String(50), nullable=False, index=True)
    confidence = Column(String(20), index=True)

    ingested_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "latitude", "longitude", "acq_date", "acq_time", "source_sensor",
            name="uq_firms_location_time_sensor",
        ),
    )


class TikTokVideo(Base):
    """TikTok videos from keyword, hashtag, and watchlist monitoring."""
    __tablename__ = "tiktok_videos"

    id = Column(Integer, primary_key=True, autoincrement=True)
    video_id = Column(String(50), unique=True, nullable=False, index=True)

    author = Column(String(100))
    author_id = Column(String(50))

    description = Column(Text)
    hashtags = Column(Text)
    sound_id = Column(String(50))
    sound_title = Column(Text)

    posted_at = Column(DateTime, index=True)
    collected_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    source_type = Column(String(50), index=True)
    source_query = Column(String(200), index=True)

    likes = Column(Integer, default=0)
    comments = Column(Integer, default=0)
    shares = Column(Integer, default=0)
    views = Column(Integer, default=0)
    duration = Column(Integer, default=0)

    video_url = Column(Text)          # local path after download
    thumbnail_url = Column(Text)
    video_downloaded = Column(Boolean, default=False)

    # AI-extracted fields (populated later)
    ai_event_type = Column(String(50))
    ai_location = Column(Text)
    ai_severity = Column(String(20))
    ai_confidence = Column(Float)
    ai_summary = Column(Text)
    ai_transcript = Column(Text)
    ai_actors = Column(Text)
    ai_processed_at = Column(DateTime)

    raw_json = Column(JSON)

    __table_args__ = (
        Index("idx_tiktok_source", "source_type", "source_query"),
    )


class TwitterPost(Base):
    """Twitter/X posts from keyword search and user monitoring."""
    __tablename__ = "twitter_posts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tweet_id = Column(String(50), unique=True, nullable=False, index=True)

    author_handle = Column(String(100), index=True)
    author_name = Column(String(200))
    followers_count = Column(Integer, default=0)
    verified = Column(Boolean, default=False)

    content = Column(Text)
    language = Column(String(10))

    posted_at = Column(DateTime, index=True)
    collected_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    search_query = Column(String(200), index=True)

    likes = Column(Integer, default=0)
    retweets = Column(Integer, default=0)
    replies = Column(Integer, default=0)
    views = Column(Integer, default=0)

    is_retweet = Column(Boolean, default=False)
    is_reply = Column(Boolean, default=False)

    has_media = Column(Boolean, default=False)
    media_urls = Column(JSON)

    # AI-extracted fields
    ai_event_type = Column(String(50))
    ai_location = Column(Text)
    ai_severity = Column(String(20))
    ai_confidence = Column(Float)
    ai_summary = Column(Text)
    ai_actors = Column(Text)
    ai_processed_at = Column(DateTime)

    # Pipeline Stage 1: Filter
    pipeline_status = Column(String(20), index=True)       # NULL, filtered_in, filtered_out
    pipeline_filter_method = Column(String(20))             # keyword, ai, default
    pipeline_run_id = Column(Integer, index=True)

    # Pipeline Stage 2: Classify
    ai_state = Column(String(100), index=True)              # Nigerian state (normalized)
    ai_lga = Column(String(100))
    ai_incident_type = Column(String(50), index=True)       # granular incident type
    ai_category = Column(String(50))                        # report, warning, tension, arrest, etc.
    ai_fatalities_mentioned = Column(Integer)
    ai_is_original_report = Column(Boolean)
    ai_classified_at = Column(DateTime, index=True)

    raw_json = Column(JSON)


class Event(Base):
    """Correlated intelligence events — the core product of SENTINEL."""
    __tablename__ = "events"

    id = Column(Integer, primary_key=True, autoincrement=True)

    title = Column(String(300), nullable=False)
    event_type = Column(String(50), index=True)        # attack, kidnapping, protest, military_op, ied, airstrike, threat
    severity = Column(String(20), index=True)           # critical, high, moderate, low
    status = Column(String(20), default="active", index=True)  # active, resolved, false_positive

    # Confidence
    confidence_label = Column(String(20), index=True)   # unverified, moderate, high, confirmed
    confidence_score = Column(Float, default=0.0)        # 0.0 - 1.0

    # Location
    location_name = Column(Text)
    admin1 = Column(String(100), index=True)            # State
    admin2 = Column(String(100))                        # LGA
    latitude = Column(Float)
    longitude = Column(Float)

    # Details
    summary = Column(Text)
    actors = Column(Text)                               # comma-separated
    fatality_estimate = Column(Integer)

    # Source counts
    twitter_sources = Column(Integer, default=0)
    firms_sources = Column(Integer, default=0)
    acled_sources = Column(Integer, default=0)

    # Timing
    event_time = Column(DateTime)                       # when the event actually happened
    first_reported = Column(DateTime)                   # when first source appeared
    last_updated = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class EventSource(Base):
    """Links events to their source records (tweets, hotspots, ACLED)."""
    __tablename__ = "event_sources"

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(Integer, nullable=False, index=True)
    source_type = Column(String(20), nullable=False)    # twitter, firms, acled
    source_id = Column(String(50), nullable=False)      # tweet_id, hotspot id, acled event_id
    added_at = Column(DateTime, default=datetime.utcnow)


class SyncRun(Base):
    """Tracks all data collection runs."""
    __tablename__ = "sync_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)

    source = Column(String(50), nullable=False, index=True)
    started_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    completed_at = Column(DateTime)
    status = Column(String(20), default="running", nullable=False, index=True)

    records_fetched = Column(Integer, default=0)
    records_new = Column(Integer, default=0)
    records_updated = Column(Integer, default=0)
    records_error = Column(Integer, default=0)

    run_metadata = Column(JSON)
    error_message = Column(Text)

    __table_args__ = (
        Index("idx_sync_source_status", "source", "status"),
    )


class PipelineRun(Base):
    """Tracks each full pipeline execution with per-stage metrics."""
    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    started_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    completed_at = Column(DateTime)
    status = Column(String(20), default="running", nullable=False, index=True)

    # Stage 1: Filter
    stage1_tweets_in = Column(Integer, default=0)
    stage1_tweets_passed = Column(Integer, default=0)
    stage1_tweets_filtered = Column(Integer, default=0)
    stage1_completed_at = Column(DateTime)

    # Stage 2: Classify
    stage2_tweets_classified = Column(Integer, default=0)
    stage2_events_created = Column(Integer, default=0)
    stage2_events_updated = Column(Integer, default=0)
    stage2_completed_at = Column(DateTime)

    # Stage 3: Aggregate
    stage3_states_analyzed = Column(Integer, default=0)
    stage3_states_flagged = Column(Integer, default=0)
    stage3_completed_at = Column(DateTime)

    # Stage 4: Assess
    stage4_assessments_created = Column(Integer, default=0)
    stage4_completed_at = Column(DateTime)

    # Stage 5: Alert
    stage5_alerts_created = Column(Integer, default=0)
    stage5_completed_at = Column(DateTime)

    error_message = Column(Text)
    run_metadata = Column(JSON)


class StateThreatLevel(Base):
    """Current threat posture per Nigerian state. One row per state."""
    __tablename__ = "state_threat_levels"

    id = Column(Integer, primary_key=True, autoincrement=True)
    state = Column(String(100), unique=True, nullable=False, index=True)

    threat_level = Column(String(20), default="NORMAL", nullable=False)

    # Aggregated metrics (updated by Stage 3)
    incident_count_window = Column(Integer, default=0)
    incident_rate = Column(Float, default=0.0)
    baseline_rate = Column(Float, default=0.0)
    acceleration = Column(Float, default=0.0)
    severity_distribution = Column(JSON)
    category_mix = Column(JSON)
    lgas_affected = Column(Integer, default=0)
    repeat_lgas = Column(JSON)
    fatalities_window = Column(Integer, default=0)

    needs_assessment = Column(Boolean, default=False)
    last_assessment_id = Column(Integer)
    last_assessment_at = Column(DateTime)

    updated_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_state_threat", "state", "threat_level"),
    )


class ThreatAssessment(Base):
    """AI-produced threat assessment for a state. Immutable record."""
    __tablename__ = "threat_assessments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_run_id = Column(Integer, nullable=False, index=True)
    state = Column(String(100), nullable=False, index=True)

    threat_level = Column(String(20), nullable=False)
    previous_threat_level = Column(String(20))
    primary_threat_areas = Column(JSON)
    threat_timeframe = Column(String(200))
    key_indicators = Column(JSON)
    specific_warnings = Column(JSON)
    recommended_actions = Column(JSON)
    narrative_summary = Column(Text)

    incident_count = Column(Integer, default=0)
    tweets_analyzed = Column(Integer, default=0)
    events_referenced = Column(JSON)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class ThreatAlert(Base):
    """Generated when a state's threat level escalates."""
    __tablename__ = "threat_alerts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    assessment_id = Column(Integer, nullable=False, index=True)
    pipeline_run_id = Column(Integer, nullable=False, index=True)
    state = Column(String(100), nullable=False, index=True)

    alert_type = Column(String(50), nullable=False)
    severity = Column(String(20), nullable=False)

    previous_level = Column(String(20))
    new_level = Column(String(20), nullable=False)

    title = Column(String(300), nullable=False)
    summary = Column(Text)
    primary_threat_areas = Column(JSON)
    recommended_actions = Column(JSON)

    acknowledged = Column(Boolean, default=False)
    acknowledged_at = Column(DateTime)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
