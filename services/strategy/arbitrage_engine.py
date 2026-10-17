import math
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional
import structlog

from knowledge_graph import MappingNode, knowledge_graph
from models import (
    AIImpactAnalysis,
    ArbitrageSignalOutput,
    MarketQuoteSnapshot,
    RelationshipType,
    SignalAction,
)

logger = structlog.get_logger(__name__)


class CrossBorderArbitrageEngine:
    """
    Core quantitative pricing and dislocation discovery engine.
    Computes theoretical fair values, microstructural hurdles,
    and directional trade signals with calibrated confidence scores.
    """

    def __init__(
        self,
        min_hurdle_bps: int = 20,       # Minimum required net alpha after all frictions
        max_allowed_spread_bps: int = 150, # Pre-market illiquidity circuit breaker
        safety_buffer_bps: int = 10,    # Extra conservatism buffer
    ):
        self.min_hurdle_bps = min_hurdle_bps
        self.max_allowed_spread_bps = max_allowed_spread_bps
        self.safety_buffer_bps = safety_buffer_bps

    def evaluate_opportunity(
        self,
        impact: AIImpactAnalysis,
        quotes: MarketQuoteSnapshot,
    ) -> ArbitrageSignalOutput:
        """
        Evaluate pricing inefficiency between an international news catalyst
        and the corresponding U.S. instrument's pre-market quote.
        """
        signal_id = f"sig-{uuid.uuid4().hex[:10]}"
        now = datetime.now(timezone.utc)

        # 1. Resolve Relationship in Knowledge Graph
        related_nodes = knowledge_graph.get_related_us_assets(impact.primary_foreign_ticker)
        mapping: Optional[MappingNode] = next(
            (node for node in related_nodes if node.us_ticker.upper() == quotes.symbol.upper()),
            None,
        )

        # Default fallback mapping if not explicitly registered in graph
        if not mapping:
            mapping = MappingNode(
                foreign_ticker=impact.primary_foreign_ticker,
                foreign_exchange="UNKNOWN",
                us_ticker=quotes.symbol,
                relationship=RelationshipType.SECTOR_PEER,
                currency_code="USD",
                beta_transmission=0.70,
                decay_halflife_minutes=20.0,
            )

        # 2. Time-Decay Weighting (\lambda(t) = 2^(-dt / halflife))
        dt_minutes = max(0.0, (now - impact.news_timestamp).total_seconds() / 60.0)
        time_decay = math.exp(-0.69315 * (dt_minutes / mapping.decay_halflife_minutes))

        # 3. Microstructure Friction Calculations
        mid_price = (quotes.premarket_bid + quotes.premarket_ask) / 2.0
        spread_dollars = quotes.premarket_ask - quotes.premarket_bid
        spread_bps = int(round((spread_dollars / mid_price) * 10000.0))

        # 4. Theoretical Fair Value Derivation
        if mapping.relationship in (RelationshipType.DIRECT_ADR, RelationshipType.DUAL_LISTED) and quotes.foreign_last_price:
            # Direct Parity Formula: (P_foreign / FX) * ADR_Ratio
            # Example: 7203.T (Toyota) in JPY / 150 (USD/JPY) * 10 (ADR ratio)
            theoretical_fair_value = (quotes.foreign_last_price / quotes.fx_rate) * quotes.adr_ratio
            valuation_method = "DIRECT_PARITY_FX"
        else:
            # Transmission Spillover Formula:
            # Expected US % move = Foreign % Move * Beta * AI Confidence * TimeDecay
            effective_move_pct = (impact.foreign_price_change_pct / 100.0) * mapping.beta_transmission * impact.confidence * time_decay
            theoretical_fair_value = quotes.prior_us_close * (1.0 + effective_move_pct)
            valuation_method = "SPILLOVER_BETA_TRANSMISSION"

        theoretical_fair_value = round(theoretical_fair_value, 3)

        # 5. Pre-market Spread Sanity Gate
        if spread_bps > self.max_allowed_spread_bps:
            return ArbitrageSignalOutput(
                signal_id=signal_id,
                action=SignalAction.HOLD,
                target_us_symbol=quotes.symbol,
                foreign_source_ticker=impact.primary_foreign_ticker,
                relationship=mapping.relationship,
                fair_value=theoretical_fair_value,
                current_premarket_price=round(mid_price, 3),
                dislocation_bps=0,
                spread_bps=spread_bps,
                expected_net_alpha_bps=0,
                confidence_pct=0.0,
                signal_urgency="LOW",
                decay_halflife_minutes=mapping.decay_halflife_minutes,
                rationale=f"Pre-market spread ({spread_bps} bps) exceeds maximum liquidity threshold ({self.max_allowed_spread_bps} bps). Risk of adverse selection.",
                parameters_used={"valuation_method": valuation_method, "dt_minutes": round(dt_minutes, 1)},
            )

        # 6. Dislocation & Net Alpha Evaluation
        # Estimated execution slippage based on book width
        estimated_slippage_bps = max(5, int(spread_bps * 0.25))

        if theoretical_fair_value > quotes.premarket_ask:
            # Opportunity to BUY: Market is trading cheaper than implied fair value
            gross_dislocation_bps = int(round(((theoretical_fair_value - quotes.premarket_ask) / quotes.premarket_ask) * 10000.0))
            net_alpha_bps = gross_dislocation_bps - (int(spread_bps / 2) + estimated_slippage_bps + self.safety_buffer_bps)

            if net_alpha_bps >= self.min_hurdle_bps and impact.sentiment_score > 0.15:
                action = SignalAction.BUY
                urgency = "HIGH" if (dt_minutes < 10.0 and net_alpha_bps > 50) else "MEDIUM"
                rationale = (
                    f"Bullish dislocation detected on {quotes.symbol}. "
                    f"Implied Fair Value ${theoretical_fair_value:.2f} exceeds Pre-Market Ask ${quotes.premarket_ask:.2f} "
                    f"by {gross_dislocation_bps} bps. Net edge after spread & slippage: +{net_alpha_bps} bps."
                )
            else:
                action = SignalAction.HOLD
                urgency = "LOW"
                rationale = (
                    f"Gross dislocation (+{gross_dislocation_bps} bps) insufficient to overcome "
                    f"hurdle and friction costs (+{net_alpha_bps} bps net alpha vs required {self.min_hurdle_bps} bps)."
                )

        elif theoretical_fair_value < quotes.premarket_bid:
            # Opportunity to SELL / SHORT: Market is trading higher than implied fair value
            gross_dislocation_bps = int(round(((quotes.premarket_bid - theoretical_fair_value) / quotes.premarket_bid) * 10000.0))
            net_alpha_bps = gross_dislocation_bps - (int(spread_bps / 2) + estimated_slippage_bps + self.safety_buffer_bps)

            if net_alpha_bps >= self.min_hurdle_bps and impact.sentiment_score < -0.15:
                action = SignalAction.SELL
                urgency = "HIGH" if (dt_minutes < 10.0 and net_alpha_bps > 50) else "MEDIUM"
                rationale = (
                    f"Bearish dislocation detected on {quotes.symbol}. "
                    f"Pre-Market Bid ${quotes.premarket_bid:.2f} exceeds Implied Fair Value ${theoretical_fair_value:.2f} "
                    f"by {gross_dislocation_bps} bps. Net edge: +{net_alpha_bps} bps."
                )
            else:
                action = SignalAction.HOLD
                urgency = "LOW"
                rationale = (
                    f"Gross bearish dislocation (+{gross_dislocation_bps} bps) insufficient to overcome "
                    f"friction hurdle (+{net_alpha_bps} bps net alpha)."
                )
        else:
            # Fair value sits comfortably inside the pre-market bid-ask spread
            gross_dislocation_bps = 0
            net_alpha_bps = 0
            action = SignalAction.HOLD
            urgency = "LOW"
            rationale = (
                f"No pricing dislocation. Fair Value ${theoretical_fair_value:.2f} is bracketed inside "
                f"the current pre-market spread (${quotes.premarket_bid:.2f} - ${quotes.premarket_ask:.2f})."
            )

        # 7. Aggregate Signal Confidence Score (0.0 to 100.0%)
        # Multi-factor weighting: AI certainty (45%), Net Edge Quality (35%), Market Liquidity (20%)
        edge_quality = min(1.0, max(0.0, net_alpha_bps / 100.0))
        liquidity_score = max(0.0, 1.0 - (spread_bps / float(self.max_allowed_spread_bps)))
        
        raw_confidence = (
            (0.45 * impact.confidence) +
            (0.35 * edge_quality) +
            (0.20 * liquidity_score)
        ) * time_decay

        confidence_pct = round(min(98.5, max(10.0, raw_confidence * 100.0)), 1)
        if action == SignalAction.HOLD:
            confidence_pct = min(confidence_pct, 40.0)

        return ArbitrageSignalOutput(
            signal_id=signal_id,
            action=action,
            target_us_symbol=quotes.symbol,
            foreign_source_ticker=impact.primary_foreign_ticker,
            relationship=mapping.relationship,
            fair_value=theoretical_fair_value,
            current_premarket_price=round(mid_price, 3),
            dislocation_bps=gross_dislocation_bps,
            spread_bps=spread_bps,
            expected_net_alpha_bps=net_alpha_bps,
            confidence_pct=confidence_pct,
            signal_urgency=urgency,
            decay_halflife_minutes=mapping.decay_halflife_minutes,
            rationale=rationale,
            parameters_used={
                "valuation_method": valuation_method,
                "beta": mapping.beta_transmission,
                "time_decay_factor": round(time_decay, 3),
                "age_minutes": round(dt_minutes, 1),
            },
        )


arbitrage_engine = CrossBorderArbitrageEngine()
