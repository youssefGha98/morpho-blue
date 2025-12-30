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
from morpho_blue.domain.market_ledger import AttributionFeatures, MarketLedger
from morpho_blue.transformations import (
    build_market_ledger_raw,
    enrich_market_ledger_with_states,
    aggregate_ledger_to_ticks,
    compute_market_indicators,
    compute_attribution_features,
    validate_attribution_integrity,
    AttributionWindowSpec,
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

    def _fetch_market_events(self, *, market_id: str) -> MarketEventTables:
        """
        Fetch raw event data for a market from repository.
        
        Args:
            market_id: Market identifier
        
        Returns:
            MarketEventTables with all event types
        """
        return MarketEventTables(
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

    def build_enriched_ledger(self, *, market_id: str) -> MarketLedger:
        """
        Build enriched market ledger with cumulative states.
        
        Pipeline:
            1. Fetch raw events from repository
            2. Standardize events into unified ledger
            3. Enrich with cumulative states
        
        Args:
            market_id: Market identifier
        
        Returns:
            MarketLedger with cumulative states and metrics
        """
        # Step 1: Fetch raw event data
        events = self._fetch_market_events(market_id=market_id)
        
        # Step 2: Standardize into unified ledger
        raw_ledger = build_market_ledger_raw(events=events)
        
        # Step 3: Enrich with states
        ledger = enrich_market_ledger_with_states(self.con, raw_ledger=raw_ledger)
        
        return ledger

    def build_tick_ledger(
        self,
        *,
        market_id: str,
        tick_seconds: int = 300,
        aggregation_level: str = "tick",
    ) -> MarketLedger:
        """
        Build tick-aggregated or block-aggregated ledger for efficient analysis.
        
        Dramatically reduces data size by aggregating events within time windows while
        preserving all information needed for attribution and rolling window analysis.
        
        Pipeline:
            1. Fetch raw events from repository
            2. Standardize events into unified ledger
            3. Enrich with cumulative states (event-level)
            4. Aggregate to tick/block level
        
        Aggregation produces:
            - 1 row per tick/block (vs N rows for N events)
            - Summed deltas (delta_supply_assets, delta_borrow_assets, etc.)
            - Final states at end of tick/block
            - Aggregated flows (supply_in_assets, withdraw_out_assets, etc.)
            - Empty ticks filled with zeros and forward-filled states
        
        Data reduction example:
            - Event-level: 10,000 rows
            - 5min tick-level: ~100 rows (99% reduction)
            - Block-level: ~1,000 rows (90% reduction)
        
        Args:
            market_id: Market identifier
            tick_seconds: Tick duration in seconds (default 300 = 5min)
            aggregation_level: "tick" for time-based ticks, "block" for block-level
        
        Returns:
            MarketLedger aggregated at tick or block level with flow columns
        """
        # Build event-level enriched ledger first
        ledger = self.build_enriched_ledger(market_id=market_id)
        
        # Aggregate to tick/block level
        tick_ledger = aggregate_ledger_to_ticks(
            self.con,
            ledger=ledger,
            tick_seconds=tick_seconds,
            aggregation_level=aggregation_level,
        )
        
        return tick_ledger

    def build_market_dataset(self, *, market_id: str) -> pa.Table:
        """
        Build complete indicator dataset for a market.
        
        Pipeline:
            1. Build enriched ledger (states)
            2. Compute advanced indicators
        
        Args:
            market_id: Market identifier
        
        Returns:
            PyArrow table with full indicator dataset
        """
        # Step 1: Build enriched ledger
        ledger = self.build_enriched_ledger(market_id=market_id)
        
        # Step 2: Compute indicators
        indicators = compute_market_indicators(ledger=ledger)

        return indicators.table

    def build_attribution_dataset(
        self,
        *,
        market_id: str | None = None,
        ledger: MarketLedger | None = None,
        windows: AttributionWindowSpec = AttributionWindowSpec(),
        compute_windows: list[str] | None = None,
        validate: bool = True,
        preprocess_ticks: bool = True,
        tick_seconds: int = 300,
        batch_size: int | None = None,
    ) -> AttributionFeatures:
        """
        Build comprehensive attribution feature dataset.
        
        Can work in two modes:
            1. From market_id: Fetches data and builds full pipeline
            2. From ledger: Uses pre-built enriched ledger (faster, for testing)
        
        Extends the ledger with:
            - Flow decomposition into principal components
            - Utilization contribution attribution
            - IRM slope diagnostics
            - Extended rolling windows (5m to 365d)
            - Volatility attribution shares
            - Accounting integrity residuals
        
        Args:
            market_id: Market identifier (required if ledger is None)
            ledger: Pre-built enriched ledger (optional, for reuse)
            windows: Window specifications for rolling metrics
            compute_windows: Optional list of specific windows to compute (e.g., ['5min', '1h', '24h']).
                           Overrides windows.compute_windows. If None, uses all windows or windows.compute_windows.
            validate: Whether to run validation checks
            preprocess_ticks: If True, reduce dataset by keeping only first/last rows per tick (default True)
            tick_seconds: Tick size in seconds for preprocessing (default 300 = 5min)
            batch_size: If set, process rolling windows in batches to reduce memory usage
        
        Returns:
            AttributionFeatures with full feature table
        
        Raises:
            ValueError: If neither market_id nor ledger is provided
        """
        # Validate inputs
        if market_id is None and ledger is None:
            raise ValueError("Must provide either market_id or ledger")
        
        # Build or use provided ledger
        if ledger is None:
            ledger = self.build_enriched_ledger(market_id=market_id)
        
        # Compute attribution features
        attribution_table = compute_attribution_features(
            ledger=ledger,
            windows=windows,
            compute_windows=compute_windows,
            preprocess_ticks=preprocess_ticks,
            tick_seconds=tick_seconds,
            batch_size=batch_size,
        )
        
        # Step 5: Validate (optional)
        if validate:
            validation_results = validate_attribution_integrity(table=attribution_table)
            
            if not validation_results["passed"]:
                print("⚠️  Attribution validation FAILED:")
                for error in validation_results["errors"]:
                    print(f"  ❌ {error}")
            
            if validation_results["warnings"]:
                print("⚠️  Attribution validation warnings:")
                for warning in validation_results["warnings"]:
                    print(f"  ⚠️  {warning}")
        
        return AttributionFeatures(table=attribution_table)
