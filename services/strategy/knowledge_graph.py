from typing import Dict, List, Optional
from pydantic import BaseModel
from models import RelationshipType


class MappingNode(BaseModel):
    foreign_ticker: str
    foreign_exchange: str
    us_ticker: str
    relationship: RelationshipType
    currency_code: str               # 'JPY', 'EUR', 'GBP', 'AUD', 'SAR'
    adr_ratio: float = 1.0           # Shares per ADR
    beta_transmission: float = 1.0   # Transmission elasticity
    decay_halflife_minutes: float = 20.0 # Time until edge dissipates


class CrossBorderKnowledgeGraph:
    """
    In-memory knowledge graph and adjacency index mapping non-US equities,
    geopolitical catalysts, and supply chain hubs to U.S.-listed trading instruments.
    """

    def __init__(self):
        self._mappings: Dict[str, List[MappingNode]] = {}
        self._load_default_matrix()

    def _load_default_matrix(self):
        # 1. Japanese Semiconductor & Tech (Tokyo Stock Exchange)
        # 8035.T = Tokyo Electron (World's #3 semiconductor equipment maker)
        self.register(MappingNode(
            foreign_ticker="8035.T",
            foreign_exchange="TSE",
            us_ticker="AMAT",
            relationship=RelationshipType.DIRECT_COMPETITOR,
            currency_code="JPY",
            beta_transmission=0.88,
            decay_halflife_minutes=25.0,
        ))
        self.register(MappingNode(
            foreign_ticker="8035.T",
            foreign_exchange="TSE",
            us_ticker="LRCX",
            relationship=RelationshipType.DIRECT_COMPETITOR,
            currency_code="JPY",
            beta_transmission=0.85,
            decay_halflife_minutes=25.0,
        ))
        self.register(MappingNode(
            foreign_ticker="8035.T",
            foreign_exchange="TSE",
            us_ticker="NVDA",
            relationship=RelationshipType.SUPPLY_CHAIN_CUSTOMER,
            currency_code="JPY",
            beta_transmission=0.72,
            decay_halflife_minutes=30.0,
        ))
        self.register(MappingNode(
            foreign_ticker="8035.T",
            foreign_exchange="TSE",
            us_ticker="TSM",
            relationship=RelationshipType.SUPPLY_CHAIN_CUSTOMER,
            currency_code="JPY",
            beta_transmission=0.78,
            decay_halflife_minutes=30.0,
        ))

        # 7203.T = Toyota Motor Corporation
        self.register(MappingNode(
            foreign_ticker="7203.T",
            foreign_exchange="TSE",
            us_ticker="TM",
            relationship=RelationshipType.DIRECT_ADR,
            currency_code="JPY",
            adr_ratio=10.0, # 1 ADR = 10 common shares
            beta_transmission=1.00,
            decay_halflife_minutes=15.0,
        ))

        # 6758.T = Sony Group
        self.register(MappingNode(
            foreign_ticker="6758.T",
            foreign_exchange="TSE",
            us_ticker="SONY",
            relationship=RelationshipType.DIRECT_ADR,
            currency_code="JPY",
            adr_ratio=1.0,
            beta_transmission=1.00,
            decay_halflife_minutes=15.0,
        ))

        # 2. European Equities (Frankfurt, Amsterdam, London)
        # ASML.AS = ASML Holding NV
        self.register(MappingNode(
            foreign_ticker="ASML.AS",
            foreign_exchange="EURONEXT",
            us_ticker="ASML",
            relationship=RelationshipType.DUAL_LISTED,
            currency_code="EUR",
            adr_ratio=1.0,
            beta_transmission=1.00,
            decay_halflife_minutes=10.0,
        ))
        self.register(MappingNode(
            foreign_ticker="ASML.AS",
            foreign_exchange="EURONEXT",
            us_ticker="AMAT",
            relationship=RelationshipType.DIRECT_COMPETITOR,
            currency_code="EUR",
            beta_transmission=0.82,
            decay_halflife_minutes=25.0,
        ))

        # SAP.DE = SAP SE
        self.register(MappingNode(
            foreign_ticker="SAP.DE",
            foreign_exchange="XETRA",
            us_ticker="SAP",
            relationship=RelationshipType.DIRECT_ADR,
            currency_code="EUR",
            adr_ratio=1.0,
            beta_transmission=1.00,
            decay_halflife_minutes=15.0,
        ))
        self.register(MappingNode(
            foreign_ticker="SAP.DE",
            foreign_exchange="XETRA",
            us_ticker="ORCL",
            relationship=RelationshipType.DIRECT_COMPETITOR,
            currency_code="EUR",
            beta_transmission=0.65,
            decay_halflife_minutes=35.0,
        ))

        # 3. Middle Eastern Energy & Shipping (Saudi Tadawul / Red Sea)
        # 2222.SR = Saudi Aramco
        self.register(MappingNode(
            foreign_ticker="2222.SR",
            foreign_exchange="TADAWUL",
            us_ticker="USO",
            relationship=RelationshipType.MACRO_COMMODITY_PROXY,
            currency_code="SAR",
            beta_transmission=1.15,
            decay_halflife_minutes=45.0,
        ))
        self.register(MappingNode(
            foreign_ticker="2222.SR",
            foreign_exchange="TADAWUL",
            us_ticker="XLE",
            relationship=RelationshipType.SECTOR_PEER,
            currency_code="SAR",
            beta_transmission=0.75,
            decay_halflife_minutes=45.0,
        ))
        self.register(MappingNode(
            foreign_ticker="2222.SR",
            foreign_exchange="TADAWUL",
            us_ticker="CVX",
            relationship=RelationshipType.DIRECT_COMPETITOR,
            currency_code="SAR",
            beta_transmission=0.70,
            decay_halflife_minutes=40.0,
        ))

    def register(self, node: MappingNode):
        ticker_key = node.foreign_ticker.upper()
        if ticker_key not in self._mappings:
            self._mappings[ticker_key] = []
        self._mappings[ticker_key].append(node)

    def get_related_us_assets(self, foreign_ticker: str) -> List[MappingNode]:
        """Retrieve all registered U.S. instruments influenced by the foreign asset."""
        return self._mappings.get(foreign_ticker.upper(), [])


knowledge_graph = CrossBorderKnowledgeGraph()
