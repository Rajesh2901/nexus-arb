from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class RegionEnum(str, Enum):
    JAPAN = "APAC_JAPAN"
    EUROPE = "EMEA_EUROPE"
    AUSTRALIA = "APAC_AUSTRALIA"
    MIDDLE_EAST = "MENA_MIDDLE_EAST"
    GLOBAL = "GLOBAL_WIRE"


class RawNewsItem(BaseModel):
    id: str = Field(..., description="Unique deterministic hash of source and title")
    source: str = Field(..., description="Feed source identifier (e.g. TDNET, EQS, ASX, TADAWUL)")
    region: RegionEnum = Field(..., description="Geographic origin region")
    language: str = Field(default="en", description="ISO 639-1 language code ('ja', 'de', 'ar', 'en')")
    title: str = Field(..., description="Article headline")
    body: Optional[str] = Field(default=None, description="Article summary or body text")
    symbols: List[str] = Field(default_factory=list, description="Referenced stock symbols / tickers")
    url: Optional[str] = Field(default=None, description="Source article URL if available")
    published_at: datetime = Field(..., description="Reported publication time in UTC")
    ingested_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp when event entered the ingestion pipeline",
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary feed metadata (e.g. category, priority, raw author)",
    )


class ManualNewsInjectionRequest(BaseModel):
    source: str = Field(default="MANUAL_SIM", description="Origin source name")
    region: RegionEnum = Field(default=RegionEnum.JAPAN, description="Region")
    language: str = Field(default="en", description="Language code")
    title: str = Field(..., description="Headline text for the news catalyst")
    body: Optional[str] = Field(default=None, description="Additional context or disclosure body")
    symbols: List[str] = Field(default_factory=list, description="Target foreign or US symbols")


class IngestionMetrics(BaseModel):
    total_ingested: int
    duplicates_suppressed: int
    by_region: Dict[str, int]
    last_event_time: Optional[datetime]
    redis_connected: bool
    active_collectors: List[str]
