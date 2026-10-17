from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator


class SignalAction(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


class RelationshipType(str, Enum):
    DIRECT_ADR = "DIRECT_ADR"
    DUAL_LISTED = "DUAL_LISTED"
    SUPPLY_CHAIN_CUSTOMER = "SUPPLY_CHAIN_CUSTOMER"
    SUPPLY_CHAIN_SUPPLIER = "SUPPLY_CHAIN_SUPPLIER"
    DIRECT_COMPETITOR = "DIRECT_COMPETITOR"
    SECTOR_PEER = "SECTOR_PEER"
    MACRO_COMMODITY_PROXY = "MACRO_COMMODITY_PROXY"


class AIImpactAnalysis(BaseModel):
    """Structured output emitted by the LLM News Intelligence Tier."""
    news_id: str = Field(..., description="Unique ID of source news item")
    primary_foreign_ticker: str = Field(..., description="e.g. '8035.T', 'ASML.AS', '7203.T'")
    event_category: str = Field(..., description="'EARNINGS_SURPRISE', 'CAPEX_EXPANSION', 'GUIDANCE_CUT'")
    sentiment_score: float = Field(
        ...,
        ge=-1.0,
        le=1.0,
        description="Directional sentiment: -1.0 (extreme bearish) to +1.0 (extreme bullish)",
    )
    confidence: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="LLM certainty score based on hard verified facts vs rumors",
    )
    foreign_price_change_pct: float = Field(
        ...,
        description="Observed price move in foreign home market session (e.g. +4.2% = 4.20)",
    )
    news_timestamp: datetime = Field(..., description="Publication time in UTC")
    headline: str = Field(..., description="Original news headline")


class MarketQuoteSnapshot(BaseModel):
    """Real-time pre-market and foreign market quote snapshot."""
    symbol: str
    foreign_symbol: Optional[str] = None
    fx_rate: float = Field(default=1.0, gt=0, description="Exchange rate USD to Local (e.g. USD/JPY=150.0, EUR/USD=1.08)")
    adr_ratio: float = Field(default=1.0, gt=0, description="Common shares per 1 ADR unit")
    
    # Pre-market U.S. Book
    premarket_bid: float = Field(..., gt=0)
    premarket_ask: float = Field(..., gt=0)
    premarket_last: float = Field(..., gt=0)
    prior_us_close: float = Field(..., gt=0)
    premarket_volume: int = Field(default=0, ge=0)
    
    # Home Market Underlying Quote
    foreign_last_price: Optional[float] = None
    foreign_prior_close: Optional[float] = None
    
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("premarket_ask")
    def validate_ask_above_bid(cls, v, info):
        bid = info.data.get("premarket_bid")
        if bid is not None and v < bid:
            raise ValueError(f"premarket_ask ({v}) cannot be less than premarket_bid ({bid})")
        return v


class ArbitrageSignalOutput(BaseModel):
    """Final quantitative trading recommendation produced by the dislocation engine."""
    signal_id: str
    action: SignalAction
    target_us_symbol: str
    foreign_source_ticker: str
    relationship: RelationshipType
    
    # Valuation & Dislocation Metrics
    fair_value: float
    current_premarket_price: float
    dislocation_bps: int = Field(..., description="Gross pricing dislocation in basis points")
    spread_bps: int = Field(..., description="Pre-market bid-ask spread cost in basis points")
    expected_net_alpha_bps: int = Field(..., description="Expected net edge after spread and estimated slippage")
    
    # Statistical Confidence
    confidence_pct: float = Field(..., ge=0.0, le=100.0, description="Final aggregate signal confidence (0-100%)")
    signal_urgency: str = Field(..., description="'HIGH' (<5 min window), 'MEDIUM', 'LOW'")
    decay_halflife_minutes: float
    
    # Audit & Explanations
    rationale: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    parameters_used: Dict[str, Any] = Field(default_factory=dict)
