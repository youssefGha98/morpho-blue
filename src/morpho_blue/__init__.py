"""
Morpho Blue Analytics Library.

Clean architecture with clear separation:
    - data: Data access layer (ParquetRepository)
    - domain: Domain models and schemas
    - transformations: Data transformation pipeline
    - validation: Data quality checks
    - indicator_service: High-level service orchestration
"""
from __future__ import annotations

from morpho_blue.data.parquet_repository import ParquetRepository
from morpho_blue.domain.market_events import MarketEventTables
from morpho_blue.domain.market_ledger import (
    MarketIndicators,
    MarketLedger,
    MarketLedgerRaw,
)
from morpho_blue.domain.schemas import (
    market_indicators_schema,
    market_ledger_raw_schema,
    market_ledger_schema,
)
from morpho_blue.indicator_service import IndicatorService
from morpho_blue.transformations import (
    build_market_ledger_raw,
    compute_market_indicators,
    enrich_market_ledger_with_states,
)

__all__ = [
    # Data layer
    "ParquetRepository",
    # Domain models
    "MarketEventTables",
    "MarketLedgerRaw",
    "MarketLedger",
    "MarketIndicators",
    # Schemas
    "market_ledger_raw_schema",
    "market_ledger_schema",
    "market_indicators_schema",
    # Transformations
    "build_market_ledger_raw",
    "enrich_market_ledger_with_states",
    "compute_market_indicators",
    # Service
    "IndicatorService",
]


def hello() -> str:
    return "Hello from morpho-blue!"

