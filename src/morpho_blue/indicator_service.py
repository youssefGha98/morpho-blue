"""
Service layer for building market indicator datasets.
Orchestrates data access, transformation, and feature engineering.
"""
from __future__ import annotations

from dataclasses import dataclass

import duckdb
import pyarrow as pa

from morpho_blue.data.parquet_repository import ParquetRepository
from morpho_blue.domain.market_events import MarketEventTables
from morpho_blue.transformations import (
    build_market_ledger_raw,
    enrich_market_ledger_with_states,
    compute_market_indicators,
)


@dataclass(frozen=True)
class IndicatorService:
    """
    Service for building market indicator datasets.
    
    Responsibilities:
        - Fetch raw event data via repository
        - Orchestrate transformation pipeline
        - Return fully enriched indicator dataset
    """
    repo: ParquetRepository
    con: duckdb.DuckDBPyConnection

    def build_market_dataset(self, *, market_id: str) -> pa.Table:
        """
        Build complete indicator dataset for a market.
        
        Pipeline:
            1. Fetch raw events from repository
            2. Standardize events into unified ledger
            3. Enrich with cumulative states
            4. Compute advanced indicators
        
        Args:
            market_id: Market identifier
        
        Returns:
            PyArrow table with full indicator dataset
        """
        # Step 1: Fetch raw event data
        events = MarketEventTables(
            market_id=market_id,
            supplies=self.repo.read_event(event="Supply", market_id=market_id),
            withdraws=self.repo.read_event(event="Withdraw", market_id=market_id),
            borrows=self.repo.read_event(event="Borrow", market_id=market_id),
            repays=self.repo.read_event(event="Repay", market_id=market_id),
            liquidates=self.repo.read_event(event="Liquidate", market_id=market_id),
            accrue_interests=self.repo.read_event(event="AccrueInterest", market_id=market_id),
            collateral_supplies=self.repo.read_event(event="SupplyCollateral", market_id=market_id),
            collateral_withdraws=self.repo.read_event(event="WithdrawCollateral", market_id=market_id),
        )

        # Step 2: Standardize into unified ledger
        raw_ledger = build_market_ledger_raw(events=events)
        
        # Step 3: Enrich with states
        ledger = enrich_market_ledger_with_states(self.con, raw_ledger=raw_ledger)
        
        # Step 4: Compute indicators
        indicators = compute_market_indicators(ledger=ledger)

        return indicators.table
