import asyncio
import random
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import List, Optional
import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from config import settings
from models import RawNewsItem, RegionEnum
from publisher import publisher
from rate_limiter import limiter_registry

logger = structlog.get_logger(__name__)


class BaseNewsCollector(ABC):
    """Abstract base class for all regional and provider-specific news collectors."""

    def __init__(self, name: str, region: RegionEnum, interval_seconds: float):
        self.name = name
        self.region = region
        self.interval_seconds = interval_seconds
        self.is_running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        """Start the collector's async polling loop."""
        self.is_running = True
        self._task = asyncio.create_task(self._run_loop(), name=f"collector-{self.name}")
        logger.info("Collector started", name=self.name, region=self.region.value)

    async def stop(self):
        """Stop collector gracefully."""
        self.is_running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Collector stopped", name=self.name)

    async def _run_loop(self):
        """Continuous execution loop with error containment."""
        while self.is_running:
            try:
                await self.poll()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in collector cycle", collector=self.name, error=str(e), exc_info=True)
            await asyncio.sleep(self.interval_seconds)

    @abstractmethod
    async def poll(self):
        """Poll the target feed and publish new items."""
        pass


# ---------------------------------------------------------------------------
# Regional Realistic Feed Collectors (Japan, Europe, Australia, Middle East)
# ---------------------------------------------------------------------------

class JapanNewsCollector(BaseNewsCollector):
    """
    Monitors Tokyo Stock Exchange disclosures (TDnet) & Nikkei wire.
    Key ADR / U.S. Cross-border targets:
    - 7203.T (Toyota -> TM)
    - 8035.T (Tokyo Electron -> AMAT, LRCX, NVDA)
    - 6758.T (Sony -> SONY)
    - 9984.T (SoftBank -> ARM)
    """

    def __init__(self):
        super().__init__("japan-tdnet-nikkei", RegionEnum.JAPAN, settings.POLL_INTERVAL_JAPAN)
        self.headlines_pool = [
            ("8035.T", "Tokyo Electron raises FY operating profit forecast by 14% on advanced packaging surge", ["8035.T", "AMAT", "NVDA"]),
            ("7203.T", "Toyota Motor halts three domestic assembly lines following supplier component fire", ["7203.T", "TM"]),
            ("6758.T", "Sony Group confirms spin-off timetable for financial services unit, expects $1.2B special dividend", ["6758.T", "SONY"]),
            ("9984.T", "SoftBank Group leads $3B private funding round in next-gen photonics AI venture", ["9984.T", "ARM"]),
            ("6501.T", "Hitachi inks €2.4B grid modernization deal with German power transmission operators", ["6501.T", "ETN"]),
            ("7751.T", "Canon reveals proprietary nanoimprint lithography tool customer shipment schedule", ["7751.T", "ASML", "CAJ"]),
        ]

    async def poll(self):
        if not settings.ENABLE_SYNTHETIC_MOCKS:
            return

        # Randomly emit realistic market-moving news disclosures
        if random.random() < 0.35:
            symbol, title, peers = random.choice(self.headlines_pool)
            now = datetime.now(timezone.utc)
            item_id = publisher.generate_hash("TDNET", title)

            item = RawNewsItem(
                id=item_id,
                source="TDNET",
                region=self.region,
                language="ja",
                title=title,
                body=f"Official disclosure submitted to Tokyo Stock Exchange (TDnet). Issuer Code: {symbol}. Target peer spillovers: {', '.join(peers)}.",
                symbols=[symbol] + peers,
                published_at=now,
                metadata={"market": "TSE Prime", "disclosure_type": "TIMELY_DISCLOSURE"},
            )
            await publisher.publish_news(item)


class EuropeNewsCollector(BaseNewsCollector):
    """
    Monitors European Regulatory Feeds (EQS News / DGAP Germany, LSE RNS).
    Key ADR / U.S. Cross-border targets:
    - ASML (Euronext Amsterdam -> ASML NASDAQ)
    - SAP (XETRA Frankfurt -> SAP NYSE)
    - NOVO-B (Copenhagen -> NVO NYSE)
    - SHEL (LSE London -> SHEL NYSE)
    """

    def __init__(self):
        super().__init__("europe-eqs-rns", RegionEnum.EUROPE, settings.POLL_INTERVAL_EUROPE)
        self.headlines_pool = [
            ("ASML.AS", "ASML reports Q3 net bookings of €2.63B, trailing consensus amid customer capex reviews", ["ASML", "AMAT", "LRCX"]),
            ("SAP.DE", "SAP announces cloud revenue acceleration of 27% and raises full-year operating profit outlook", ["SAP", "ORCL", "MSFT"]),
            ("NOVO-B.CO", "Novo Nordisk Phase 3 trial shows 18.2% weight loss with oral amycretin formulation", ["NVO", "LLY"]),
            ("SHEL.L", "Shell authorizes $3.5B share buyback program as offshore gas production hits record high", ["SHEL", "XOM", "CVX"]),
            ("AIR.PA", "Airbus revises commercial aircraft delivery target downward to 770 citing CFM supply delays", ["AIR.PA", "BA", "GE"]),
        ]

    async def poll(self):
        if not settings.ENABLE_SYNTHETIC_MOCKS:
            return

        if random.random() < 0.35:
            symbol, title, peers = random.choice(self.headlines_pool)
            now = datetime.now(timezone.utc)
            item_id = publisher.generate_hash("EQS_NEWS", title)

            item = RawNewsItem(
                id=item_id,
                source="EQS_NEWS",
                region=self.region,
                language="de",
                title=title,
                body=f"Regulatory news service announcement broadcast under Art. 17 MAR. Issuer: {symbol}.",
                symbols=[symbol] + peers,
                published_at=now,
                metadata={"exchange": "Euronext/XETRA", "ad_hoc": True},
            )
            await publisher.publish_news(item)


class AustraliaNewsCollector(BaseNewsCollector):
    """
    Monitors Australian Securities Exchange (ASX) announcements.
    Key ADR / U.S. Cross-border targets:
    - BHP.AX (ASX Sydney -> BHP NYSE)
    - RIO.AX (ASX Sydney -> RIO NYSE)
    - CSL.AX (ASX Sydney -> CSLLY)
    """

    def __init__(self):
        super().__init__("australia-asx-map", RegionEnum.AUSTRALIA, settings.POLL_INTERVAL_AUSTRALIA)
        self.headlines_pool = [
            ("BHP.AX", "BHP cuts Western Australia iron ore production forecast after railway derailment", ["BHP", "VALE", "RIO"]),
            ("RIO.AX", "Rio Tinto and partners sign $6.2B final investment decision for Simandou high-grade iron project", ["RIO", "BHP"]),
            ("WDS.AX", "Woodside Energy approves $4.8B Louisiana ammonia terminal acquisition", ["WDS", "XOM"]),
            ("CSL.AX", "CSL Behring receives FDA priority review designation for novel gene therapy candidate", ["CSL.AX", "CSLLY", "PFE"]),
        ]

    async def poll(self):
        if not settings.ENABLE_SYNTHETIC_MOCKS:
            return

        if random.random() < 0.30:
            symbol, title, peers = random.choice(self.headlines_pool)
            now = datetime.now(timezone.utc)
            item_id = publisher.generate_hash("ASX_MAP", title)

            item = RawNewsItem(
                id=item_id,
                source="ASX_MAP",
                region=self.region,
                language="en",
                title=title,
                body=f"Official ASX Company Announcement. Sensitivity: Market Sensitive. Issuer: {symbol}.",
                symbols=[symbol] + peers,
                published_at=now,
                metadata={"exchange": "ASX Sydney", "price_sensitive": True},
            )
            await publisher.publish_news(item)


class MiddleEastNewsCollector(BaseNewsCollector):
    """
    Monitors Middle East Disclosures (Saudi Tadawul, UAE DFM/ADX, Argaam).
    Key Energy & Shipping Cross-border targets:
    - 2222.SR (Saudi Aramco -> USO, XLE, CVX)
    - Maritime security and oil supply chain alerts (-> FRO, STNG, ZIM)
    """

    def __init__(self):
        super().__init__("mena-tadawul-argaam", RegionEnum.MIDDLE_EAST, settings.POLL_INTERVAL_MIDDLE_EAST)
        self.headlines_pool = [
            ("2222.SR", "Saudi Aramco discovers two new natural gas fields in Eastern Province with test flows of 32M scfd", ["2222.SR", "USO", "XLE"]),
            ("TADAWUL", "OPEC+ delegates signal extension of voluntary 2.2M bpd crude cuts through Q1 next year", ["USO", "BNO", "XLE", "CVX"]),
            ("ADNOC.AD", "ADNOC Gas signs 10-year LNG supply agreement with Indian Oil Corporation valued at $9B", ["ADNOC.AD", "SHEL"]),
            ("REDSEA", "Maritime authorities issue security bulletin warning commercial vessels of drone activity near Bab el-Mandeb", ["ZIM", "FRO", "STNG"]),
        ]

    async def poll(self):
        if not settings.ENABLE_SYNTHETIC_MOCKS:
            return

        if random.random() < 0.25:
            symbol, title, peers = random.choice(self.headlines_pool)
            now = datetime.now(timezone.utc)
            item_id = publisher.generate_hash("TADAWUL_ARGAAM", title)

            item = RawNewsItem(
                id=item_id,
                source="TADAWUL_ARGAAM",
                region=self.region,
                language="ar",
                title=title,
                body=f"Regional regulatory announcement from Gulf markets. Primary identifier: {symbol}.",
                symbols=[symbol] + peers,
                published_at=now,
                metadata={"jurisdiction": "MENA/GCC", "priority": "HIGH"},
            )
            await publisher.publish_news(item)


# ---------------------------------------------------------------------------
# External Live HTTP API Collectors (Alpha Vantage & NewsAPI)
# ---------------------------------------------------------------------------

class AlphaVantageCollector(BaseNewsCollector):
    """Fetches real-time financial news from Alpha Vantage NEWS_SENTIMENT API endpoint."""

    def __init__(self):
        super().__init__("alphavantage-wire", RegionEnum.GLOBAL, settings.POLL_INTERVAL_ALPHAVANTAGE)
        self.client = httpx.AsyncClient(timeout=10.0)
        limiter_registry.register("alphavantage", settings.RATE_LIMIT_ALPHAVANTAGE_RPM, burst=1)

    async def poll(self):
        if not settings.ALPHA_VANTAGE_API_KEY or settings.ALPHA_VANTAGE_API_KEY == "demo":
            return  # Skip polling if no valid key is configured

        # Acquire token to enforce rate limit
        await limiter_registry.acquire("alphavantage")

        url = "https://www.alphavantage.co/query"
        params = {
            "function": "NEWS_SENTIMENT",
            "topics": "technology,manufacturing,energy",
            "limit": 10,
            "apikey": settings.ALPHA_VANTAGE_API_KEY,
        }

        try:
            resp = await self.client.get(url, params=params)
            if resp.status_code == 200:
                data = resp.json()
                feed = data.get("feed", [])
                for article in feed:
                    title = article.get("title", "")
                    if not title:
                        continue
                    item_id = publisher.generate_hash("ALPHA_VANTAGE", title)
                    pub_time_str = article.get("time_published", "")
                    try:
                        pub_time = datetime.strptime(pub_time_str, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
                    except Exception:
                        pub_time = datetime.now(timezone.utc)

                    symbols = [t.get("ticker", "") for t in article.get("ticker_sentiment", []) if t.get("ticker")]
                    item = RawNewsItem(
                        id=item_id,
                        source="ALPHA_VANTAGE",
                        region=RegionEnum.GLOBAL,
                        language="en",
                        title=title,
                        body=article.get("summary"),
                        symbols=symbols,
                        url=article.get("url"),
                        published_at=pub_time,
                        metadata={"overall_sentiment_score": article.get("overall_sentiment_score")},
                    )
                    await publisher.publish_news(item)
            elif resp.status_code == 429:
                logger.warning("Alpha Vantage returned 429 Too Many Requests, backing off")
        except Exception as e:
            logger.error("Alpha Vantage request failed", error=str(e))

    async def stop(self):
        await super().stop()
        await self.client.aclose()


class CollectorManager:
    """Orchestrates all active news collectors."""

    def __init__(self):
        self.collectors: List[BaseNewsCollector] = [
            JapanNewsCollector(),
            EuropeNewsCollector(),
            AustraliaNewsCollector(),
            MiddleEastNewsCollector(),
            AlphaVantageCollector(),
        ]

    async def start_all(self):
        logger.info("Initializing all news collectors...")
        for col in self.collectors:
            await col.start()

    async def stop_all(self):
        logger.info("Stopping all news collectors...")
        for col in self.collectors:
            await col.stop()

    def get_collector_names(self) -> List[str]:
        return [c.name for c in self.collectors if c.is_running]


collector_manager = CollectorManager()
