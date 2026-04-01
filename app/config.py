"""Single flat configuration — reads from .env, no config.ini, no adapters."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Database
    database_url: str = Field(
        default="sqlite+aiosqlite:///./data/sentinel.db",
        alias="DATABASE_URL",
    )

    # ACLED
    acled_email: str = Field(default="", alias="ACLED_EMAIL")
    acled_password: str = Field(default="", alias="ACLED_PASSWORD")
    acled_base_url: str = "https://acleddata.com/api/acled/read"
    acled_token_url: str = "https://acleddata.com/oauth/token"
    acled_country: str = "Nigeria"
    acled_iso: int = 566
    acled_page_limit: int = 5000
    acled_historical_years: int = 5

    # FIRMS
    firms_map_key: str = Field(default="", alias="FIRMS_MAP_KEY")
    firms_base_url: str = "https://firms.modaps.eosdis.nasa.gov"
    firms_country_code: str = "NGA"
    firms_area_bbox: str = "2.68,4.07,14.68,13.89"
    firms_sources: str = "VIIRS_NOAA20_NRT"  # comma-separated
    firms_day_range: int = 2
    firms_min_confidence: str = "nominal"

    # TikTok
    tiktok_ms_token: str = Field(default="", alias="TIKTOK_MS_TOKEN")
    tiktok_keywords_en: str = "conflict,war,protest,attack,kidnap,bandit"
    tiktok_keywords_ha: str = "yaki,gwagwarwa"
    tiktok_hashtags: str = "#Nigeria,#conflict,#ISWAP"
    tiktok_watchlist: str = ""  # comma-separated usernames
    tiktok_max_per_keyword: int = 50
    tiktok_max_per_hashtag: int = 30
    tiktok_max_per_user: int = 30
    tiktok_download_videos: bool = True
    tiktok_rate_delay_min: float = 10.0
    tiktok_rate_delay_max: float = 15.0

    # Twitter
    twitter_username: str = Field(default="", alias="TWITTER_USERNAME")
    twitter_email: str = Field(default="", alias="TWITTER_EMAIL")
    twitter_password: str = Field(default="", alias="TWITTER_PASSWORD")
    twitter_default_count: int = 50
    twitter_headless: bool = True
    twitter_scroll_delay_min: float = 3.0
    twitter_scroll_delay_max: float = 6.0

    # OpenAI (optional)
    openai_api_key: str = Field(default="", alias="OPENAI_API_KEY")

    # Pipeline
    pipeline_model_mini: str = "gpt-4.1-mini"
    pipeline_model_full: str = "gpt-4.1"
    pipeline_filter_batch_size: int = 50
    pipeline_classify_batch_size: int = 25
    pipeline_max_tweets_per_run: int = 500
    pipeline_aggregate_window_hours: int = 72
    pipeline_baseline_window_days: int = 30
    pipeline_threat_escalation_threshold: float = 2.0
    pipeline_min_incidents_for_assessment: int = 3


_settings = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
