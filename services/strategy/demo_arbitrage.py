"""
Comprehensive demonstration of the Cross-Border News Arbitrage Engine.
Simulates real-world market scenarios across Japanese semiconductors, European dual-listings,
and Middle Eastern energy spillovers.
"""
from datetime import datetime, timedelta, timezone
from arbitrage_engine import arbitrage_engine
from models import AIImpactAnalysis, MarketQuoteSnapshot


def print_signal(title: str, signal):
    print("=" * 80)
    print(f"SCENARIO: {title}")
    print("=" * 80)
    print(f"Action:                  [{signal.action.value}]")
    print(f"Target US Symbol:        {signal.target_us_symbol}")
    print(f"Catalyst Foreign Ticker: {signal.foreign_source_ticker} ({signal.relationship.value})")
    print(f"Premarket Mid:           ${signal.current_premarket_price:.2f}")
    print(f"Implied Fair Value:      ${signal.fair_value:.2f}")
    print(f"Gross Dislocation:       {signal.dislocation_bps} bps")
    print(f"Pre-market Spread:       {signal.spread_bps} bps")
    print(f"Expected Net Alpha:      +{signal.expected_net_alpha_bps} bps")
    print(f"Confidence:              {signal.confidence_pct}%")
    print(f"Urgency:                 {signal.signal_urgency}")
    print(f"Rationale:               {signal.rationale}")
    print("-" * 80 + "\n")


def run_demonstration():
    now = datetime.now(timezone.utc)

    # -----------------------------------------------------------------------
    # Scenario 1: Japanese Semiconductor Catalyst (Tokyo Electron -> NVDA, AMAT)
    # News: Tokyo Electron (8035.T) surges +5.8% on advanced packaging guidance
    # -----------------------------------------------------------------------
    jp_semi_news = AIImpactAnalysis(
        news_id="news-jp-8035-001",
        primary_foreign_ticker="8035.T",
        event_category="EARNINGS_SURPRISE",
        sentiment_score=0.88,
        confidence=0.92,
        foreign_price_change_pct=5.8,
        news_timestamp=now - timedelta(minutes=12),
        headline="Tokyo Electron raises FY operating profit forecast by 14% on advanced packaging surge",
    )

    # 1A: NVDA Pre-market quote is lagging (prior close $130.00, pre-market ask $130.80)
    # Tokyo Electron move implies ~ +2.9% on NVDA (fair value ~$133.80)
    nvda_quotes = MarketQuoteSnapshot(
        symbol="NVDA",
        premarket_bid=130.70,
        premarket_ask=130.85,
        premarket_last=130.75,
        prior_us_close=130.00,
        premarket_volume=125000,
    )
    sig_nvda = arbitrage_engine.evaluate_opportunity(jp_semi_news, nvda_quotes)
    print_signal("1A. Japanese Tech Catalyst -> NVDA Pre-Market Lag", sig_nvda)

    # 1B: AMAT (Applied Materials) - Direct Competitor with Beta = 0.88
    # Prior close $200.00, pre-market ask $201.50, fair value implies ~$208.00+
    amat_quotes = MarketQuoteSnapshot(
        symbol="AMAT",
        premarket_bid=201.20,
        premarket_ask=201.60,
        premarket_last=201.40,
        prior_us_close=200.00,
        premarket_volume=45000,
    )
    sig_amat = arbitrage_engine.evaluate_opportunity(jp_semi_news, amat_quotes)
    print_signal("1B. Japanese Tech Catalyst -> AMAT Peer Spillover", sig_amat)

    # -----------------------------------------------------------------------
    # Scenario 2: European Parity Drop (ASML Euronext Amsterdam -> NASDAQ:ASML)
    # News: ASML bookings miss consensus in Europe, shares drop -4.5% in Amsterdam
    # -----------------------------------------------------------------------
    asml_news = AIImpactAnalysis(
        news_id="news-eu-asml-002",
        primary_foreign_ticker="ASML.AS",
        event_category="GUIDANCE_CUT",
        sentiment_score=-0.82,
        confidence=0.95,
        foreign_price_change_pct=-4.5,
        news_timestamp=now - timedelta(minutes=5),
        headline="ASML reports Q3 net bookings of €2.63B, trailing consensus amid customer capex reviews",
    )

    # U.S. NASDAQ pre-market book hasn't dumped yet (Bid $710.00, Ask $711.50)
    # Foreign Euronext close is €630, EUR/USD = 1.08 -> Fair value = $680.40
    asml_quotes = MarketQuoteSnapshot(
        symbol="ASML",
        foreign_symbol="ASML.AS",
        foreign_last_price=630.00, # Euronext price in EUR
        fx_rate=0.9259,           # EUR to USD implied: 1 / 1.08
        adr_ratio=1.0,
        premarket_bid=710.00,
        premarket_ask=711.50,
        premarket_last=710.50,
        prior_us_close=712.00,
        premarket_volume=18000,
    )
    sig_asml = arbitrage_engine.evaluate_opportunity(asml_news, asml_quotes)
    print_signal("2. European Market Dislocation -> ASML NASDAQ Short/Sell", sig_asml)

    # -----------------------------------------------------------------------
    # Scenario 3: Illiquid Book / Wide Spread Circuit Breaker
    # Premarket spread is 185 bps wide (e.g. Bid $98.00, Ask $100.00)
    # System must protect capital and refuse to trigger market orders
    # -----------------------------------------------------------------------
    illiquid_quotes = MarketQuoteSnapshot(
        symbol="NVDA",
        premarket_bid=128.00,
        premarket_ask=131.50, # Wide spread (~270 bps)
        premarket_last=129.50,
        prior_us_close=130.00,
        premarket_volume=500,
    )
    sig_illiquid = arbitrage_engine.evaluate_opportunity(jp_semi_news, illiquid_quotes)
    print_signal("3. Wide Pre-Market Spread -> Circuit Breaker HOLD", sig_illiquid)


if __name__ == "__main__":
    run_demonstration()
