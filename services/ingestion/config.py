from typing import Dict
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # General Service Configuration
    SERVICE_NAME: str = "nexus-news-ingestion"
    ENVIRONMENT: str = "development"
    LOG_LEVEL: str = "INFO"
    HOST: str = "0.0.0.0"
    PORT: int = 8001

    # Redis Configuration (Pub/Sub & Streams)
    REDIS_URL: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection URL for streams and pub/sub",
    )
    REDIS_STREAM_KEY: str = "stream:news:raw"
    REDIS_PUBSUB_CHANNEL: str = "channel:news:realtime"
    DEDUP_SET_KEY: str = "set:news:seen_hashes"
    DEDUP_TTL_SECONDS: int = 86400  # 24 hours deduplication window

    # API Keys for External Providers
    ALPHA_VANTAGE_API_KEY: str = Field(
        default="demo",
        description="Alpha Vantage API key for financial news",
    )
    NEWS_API_KEY: str = Field(
        default="",
        description="NewsAPI key for broad financial press",
    )

    # Ingestion Behavior Controls
    ENABLE_SYNTHETIC_MOCKS: bool = Field(
        default=True,
        description="Emit realistic cross-border market disclosures if live feeds are idle or unauthenticated",
    )
    MOCK_INTERVAL_SECONDS: float = 8.0  # Frequency of simulated foreign disclosures

    # Polling Intervals (seconds)
    POLL_INTERVAL_JAPAN: int = 15
    POLL_INTERVAL_EUROPE: int = 15
    POLL_INTERVAL_AUSTRALIA: int = 20
    POLL_INTERVAL_MIDDLE_EAST: int = 20
    POLL_INTERVAL_ALPHAVANTAGE: int = 60

    # Rate Limiting Configuration (Max requests per minute per provider)
    RATE_LIMIT_ALPHAVANTAGE_RPM: int = 5   # Free tier allows 5 req/min
    RATE_LIMIT_NEWSAPI_RPM: int = 30
    RATE_LIMIT_RSS_RPM: int = 60


settings = Settings()
