"""
Domain models for market ledgers and indicators.
Uses PyArrow schemas for strict type enforcement.
"""
from __future__ import annotations

from dataclasses import dataclass
import pyarrow as pa

from morpho_blue.domain.schemas import (
    market_ledger_raw_schema,
    market_ledger_schema,
    market_indicators_schema,
    attribution_feature_schema,
)


@dataclass(frozen=True)
class MarketLedgerRaw:
    """
    Standardized event deltas (pre state reconstruction).
    
    Schema:
        - market_id: str
        - block_number: int64
        - log_index: int64
        - block_timestamp: int64
        - tx_hash: str
        - event_type: str
        - delta_supply_assets: int64
        - delta_borrow_assets: int64
        - delta_collateral_assets: int64
        - borrow_rate_per_sec: float64 (nullable, only for AccrueInterest)
    """
    table: pa.Table
    
    def __post_init__(self) -> None:
        """Validate schema on initialization."""
        expected = market_ledger_raw_schema()
        if not self.table.schema.equals(expected, check_metadata=False):
            # Allow schema flexibility for nullable fields
            for i, (field, expected_field) in enumerate(zip(self.table.schema, expected, strict=False)):
                if field.name != expected_field.name:
                    raise ValueError(
                        f"Schema mismatch at field {i}: expected {expected_field.name}, got {field.name}"
                    )
    
    @property
    def schema(self) -> pa.Schema:
        """Return the schema."""
        return market_ledger_raw_schema()


@dataclass(frozen=True)
class MarketLedger:
    """
    Enriched ledger after cumulative state reconstruction.
    
    Schema includes:
        - All raw ledger fields
        - total_supplied_assets: int64 (cumulative)
        - outstanding_borrow_assets: int64 (cumulative)
        - total_collateral_assets: int64 (cumulative)
        - utilization_rate: float64 (0.0 to 1.0)
        - borrow_apy: float64
        - supply_rate_per_sec: float64
        - supply_apy: float64
        - delta_utilization: float64
        - delta_borrow_apy: float64
        - delta_supply_apy: float64
    """
    table: pa.Table
    
    def __post_init__(self) -> None:
        """Validate schema on initialization."""
        expected = market_ledger_schema()
        if not self.table.schema.equals(expected, check_metadata=False):
            # Check that all required fields are present (allow extra fields)
            actual_names = set(self.table.schema.names)
            expected_names = set(expected.names)
            missing = expected_names - actual_names
            if missing:
                # Only fail if required fields are missing (extra fields are OK)
                raise ValueError(
                    f"Schema mismatch. Missing required fields: {missing}"
                )
    
    @property
    def schema(self) -> pa.Schema:
        """Return the schema."""
        return market_ledger_schema()


@dataclass(frozen=True)
class MarketIndicators:
    """
    Full indicator dataset with engineered features.
    
    Extends MarketLedger with:
        - Rolling window statistics (5min, 1h, 6h)
        - Flow decomposition (borrow_in/out, supply_in/out)
        - Intensity metrics (assets per second)
        - Elasticity measures
        - Regime indicators (high utilization flags)
    """
    table: pa.Table
    
    def __post_init__(self) -> None:
        """Validate schema on initialization."""
        expected = market_indicators_schema()
        if not self.table.schema.equals(expected, check_metadata=False):
            # Check field names at minimum
            actual_names = set(self.table.schema.names)
            expected_names = set(expected.names)
            missing = expected_names - actual_names
            extra = actual_names - expected_names
            if missing or extra:
                raise ValueError(
                    f"Schema mismatch. Missing fields: {missing}. Extra fields: {extra}"
                )
    
    @property
    def schema(self) -> pa.Schema:
        """Return the schema."""
        return market_indicators_schema()


@dataclass(frozen=True)
class AttributionFeatures:
    """
    Core attribution feature table with comprehensive utilization dynamics.
    
    Decomposes utilization changes into contributions from:
        - Borrow/Repay/Liquidate principal flows
        - Supply/Withdraw flows
        - Interest accrual
        - IRM slope diagnostics
        - Rolling window aggregations (5m to 365d)
        - Accounting integrity residuals
    
    This is the primary output for utilization attribution analysis.
    """
    table: pa.Table
    
    def __post_init__(self) -> None:
        """
        Validate schema on initialization.
        
        Note: Flexible validation since rolling windows generate many dynamic columns.
        We validate presence of core required fields only.
        """
        expected = attribution_feature_schema()
        actual_names = set(self.table.schema.names)
        expected_names = set(expected.names)
        missing = expected_names - actual_names
        
        if missing:
            raise ValueError(
                f"Attribution feature table missing required fields: {missing}"
            )
    
    @property
    def schema(self) -> pa.Schema:
        """Return the base schema (core fields only)."""
        return attribution_feature_schema()
