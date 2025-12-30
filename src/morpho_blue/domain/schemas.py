"""
PyArrow schemas for all domain models.
Defines the explicit structure of each data model using PyArrow's schema feature.
"""
from __future__ import annotations

import pyarrow as pa


# ===========================
# Event Schemas (Raw Data)
# ===========================

MARKET_EVENT_BASE_SCHEMA = pa.schema([
    pa.field("id", pa.string(), nullable=False),  # market_id
    pa.field("block_number", pa.int64(), nullable=False),
    pa.field("log_index", pa.int64(), nullable=False),
    pa.field("block_timestamp", pa.int64(), nullable=False),
    pa.field("tx_hash", pa.string(), nullable=False),
])


def supply_event_schema() -> pa.Schema:
    """Supply event schema."""
    return pa.schema([
        *MARKET_EVENT_BASE_SCHEMA,
        pa.field("assets", pa.string(), nullable=False),  # Can be hex or decimal string
    ])


def withdraw_event_schema() -> pa.Schema:
    """Withdraw event schema."""
    return pa.schema([
        *MARKET_EVENT_BASE_SCHEMA,
        pa.field("assets", pa.string(), nullable=False),
    ])


def borrow_event_schema() -> pa.Schema:
    """Borrow event schema."""
    return pa.schema([
        *MARKET_EVENT_BASE_SCHEMA,
        pa.field("assets", pa.string(), nullable=False),
    ])


def repay_event_schema() -> pa.Schema:
    """Repay event schema."""
    return pa.schema([
        *MARKET_EVENT_BASE_SCHEMA,
        pa.field("assets", pa.string(), nullable=False),
    ])


def liquidate_event_schema() -> pa.Schema:
    """Liquidate event schema."""
    return pa.schema([
        *MARKET_EVENT_BASE_SCHEMA,
        pa.field("repaidAssets", pa.string(), nullable=False),
    ])


def accrue_interest_event_schema() -> pa.Schema:
    """AccrueInterest event schema."""
    return pa.schema([
        *MARKET_EVENT_BASE_SCHEMA,
        pa.field("prevBorrowRate", pa.string(), nullable=False),
        pa.field("interest", pa.string(), nullable=False),
    ])


def supply_collateral_event_schema() -> pa.Schema:
    """SupplyCollateral event schema."""
    return pa.schema([
        *MARKET_EVENT_BASE_SCHEMA,
        pa.field("assets", pa.string(), nullable=False),
    ])


def withdraw_collateral_event_schema() -> pa.Schema:
    """WithdrawCollateral event schema."""
    return pa.schema([
        *MARKET_EVENT_BASE_SCHEMA,
        pa.field("assets", pa.string(), nullable=False),
    ])


# ===========================
# Ledger Schemas (Processed)
# ===========================

def market_ledger_raw_schema() -> pa.Schema:
    """
    Standardized event deltas (pre state reconstruction).
    This is the unified ledger format combining all event types.
    """
    return pa.schema([
        pa.field("market_id", pa.string(), nullable=False),
        pa.field("block_number", pa.int64(), nullable=False),
        pa.field("log_index", pa.int64(), nullable=False),
        pa.field("block_timestamp", pa.int64(), nullable=False),
        pa.field("tx_hash", pa.string(), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        
        # Deltas (changes in this event)
        pa.field("delta_supply_assets", pa.int64(), nullable=False),
        pa.field("delta_borrow_assets", pa.int64(), nullable=False),
        pa.field("delta_collateral_assets", pa.int64(), nullable=False),
        
        # Rate information
        pa.field("borrow_rate_per_sec", pa.float64(), nullable=True),  # Only set for AccrueInterest
    ])


def market_ledger_schema() -> pa.Schema:
    """
    Enriched ledger after cumulative state reconstruction.
    Includes running totals, utilization rates, and APYs.
    """
    return pa.schema([
        # Base fields from raw ledger
        pa.field("market_id", pa.string(), nullable=False),
        pa.field("block_number", pa.int64(), nullable=False),
        pa.field("log_index", pa.int64(), nullable=False),
        pa.field("block_timestamp", pa.int64(), nullable=False),
        pa.field("tx_hash", pa.string(), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        
        # Deltas
        pa.field("delta_supply_assets", pa.int64(), nullable=False),
        pa.field("delta_borrow_assets", pa.int64(), nullable=False),
        pa.field("delta_collateral_assets", pa.int64(), nullable=False),
        
        # Cumulative states (running totals)
        pa.field("total_supplied_assets", pa.int64(), nullable=False),
        pa.field("outstanding_borrow_assets", pa.int64(), nullable=False),
        pa.field("total_collateral_assets", pa.int64(), nullable=False),
        
        # Rates and utilization
        pa.field("borrow_rate_per_sec", pa.float64(), nullable=False),  # Forward-filled
        pa.field("utilization_rate", pa.float64(), nullable=False),  # 0.0 to 1.0
        pa.field("borrow_apy", pa.float64(), nullable=False),
        pa.field("supply_rate_per_sec", pa.float64(), nullable=False),
        pa.field("supply_apy", pa.float64(), nullable=False),
        
        # Delta metrics
        pa.field("delta_utilization", pa.float64(), nullable=False),
        pa.field("delta_borrow_apy", pa.float64(), nullable=False),
        pa.field("delta_supply_apy", pa.float64(), nullable=False),
    ])


def market_indicators_schema() -> pa.Schema:
    """
    Full indicator dataset with engineered features.
    Extends the enriched ledger with rolling window metrics and derived features.
    """
    base_fields = list(market_ledger_schema())
    
    # Additional engineered features
    engineered_fields = [
        # Instantaneous APR
        pa.field("borrow_apr", pa.float64(), nullable=False),
        
        # Flow decomposition
        pa.field("borrow_in_assets", pa.float64(), nullable=False),
        pa.field("borrow_out_assets", pa.float64(), nullable=False),
        pa.field("supply_in_assets", pa.float64(), nullable=False),
        pa.field("supply_out_assets", pa.float64(), nullable=False),
        pa.field("interest_assets", pa.float64(), nullable=False),
        
        # Rolling window metrics (5m, 1h, 6h)
        # Utilization statistics
        pa.field("util_mean_5min", pa.float64(), nullable=False),
        pa.field("util_std_5min", pa.float64(), nullable=False),
        pa.field("util_mean_1h", pa.float64(), nullable=False),
        pa.field("util_std_1h", pa.float64(), nullable=False),
        pa.field("util_mean_6h", pa.float64(), nullable=False),
        pa.field("util_std_6h", pa.float64(), nullable=False),
        
        # APR statistics
        pa.field("borrow_apr_mean_5min", pa.float64(), nullable=False),
        pa.field("borrow_apr_std_5min", pa.float64(), nullable=False),
        pa.field("borrow_apr_mean_1h", pa.float64(), nullable=False),
        pa.field("borrow_apr_std_1h", pa.float64(), nullable=False),
        pa.field("borrow_apr_mean_6h", pa.float64(), nullable=False),
        pa.field("borrow_apr_std_6h", pa.float64(), nullable=False),
        
        # Intensity metrics (assets per second)
        pa.field("borrow_intensity_5min", pa.float64(), nullable=False),
        pa.field("repay_intensity_5min", pa.float64(), nullable=False),
        pa.field("supply_intensity_5min", pa.float64(), nullable=False),
        pa.field("withdraw_intensity_5min", pa.float64(), nullable=False),
        pa.field("interest_intensity_5min", pa.float64(), nullable=False),
        
        pa.field("borrow_intensity_1h", pa.float64(), nullable=False),
        pa.field("repay_intensity_1h", pa.float64(), nullable=False),
        pa.field("supply_intensity_1h", pa.float64(), nullable=False),
        pa.field("withdraw_intensity_1h", pa.float64(), nullable=False),
        pa.field("interest_intensity_1h", pa.float64(), nullable=False),
        
        pa.field("borrow_intensity_6h", pa.float64(), nullable=False),
        pa.field("repay_intensity_6h", pa.float64(), nullable=False),
        pa.field("supply_intensity_6h", pa.float64(), nullable=False),
        pa.field("withdraw_intensity_6h", pa.float64(), nullable=False),
        pa.field("interest_intensity_6h", pa.float64(), nullable=False),
        
        # Elasticity and deltas
        pa.field("delta_borrow_apr", pa.float64(), nullable=False),
        pa.field("elasticity_apr_per_util", pa.float64(), nullable=False),
        
        # Regime indicators
        pa.field("regime_util_gt_90", pa.int8(), nullable=False),
        pa.field("regime_util_gt_95", pa.int8(), nullable=False),
        pa.field("regime_util_gt_99", pa.int8(), nullable=False),
        
        # UI-aligned metrics
        pa.field("borrow_apr_6h_avg_ui", pa.float64(), nullable=False),
    ]
    
    return pa.schema(base_fields + engineered_fields)


def attribution_feature_schema() -> pa.Schema:
    """
    Core attribution feature table schema with comprehensive utilization dynamics.
    
    Extends market_ledger_schema with:
        - State columns: buffer, headroom metrics, APR
        - Flow decomposition: borrow_in, repay_out, liquidate_repay, etc.
        - Utilization contributions: attribution terms for each flow type
        - Rate dynamics: IRM slope, factor-conditioned responses
        - Rolling windows: 5m, 1h, 6h, 24h, 7d, 30d, 90d, 365d
        - Validation: delta_u_residual for integrity checks
        - Volatility attribution shares
        - Expanding statistics
    
    Note: Schema flexibility - allow additional fields from pandas conversion.
    This function defines the minimum required fields.
    """
    base_fields = list(market_ledger_schema())
    
    # A) State columns (levels)
    state_fields = [
        pa.field("borrow_apr", pa.float64(), nullable=False),
        pa.field("buffer", pa.float64(), nullable=False),
        pa.field("headroom_u90_assets", pa.int64(), nullable=False),
        pa.field("headroom_u95_assets", pa.int64(), nullable=False),
    ]
    
    # B) Flow decomposition (per-step)
    flow_fields = [
        pa.field("borrow_in_assets", pa.float64(), nullable=False),
        pa.field("repay_out_assets", pa.float64(), nullable=False),
        pa.field("liquidate_repay_assets", pa.float64(), nullable=False),
        pa.field("supply_in_assets", pa.float64(), nullable=False),
        pa.field("withdraw_out_assets", pa.float64(), nullable=False),
        pa.field("interest_assets", pa.float64(), nullable=False),
        pa.field("net_borrow_principal_assets", pa.float64(), nullable=False),
        pa.field("net_supply_assets", pa.float64(), nullable=False),
    ]
    
    # C) Utilization contribution terms
    contrib_fields = [
        pa.field("contrib_u_from_borrow", pa.float64(), nullable=False),
        pa.field("contrib_u_from_repay", pa.float64(), nullable=False),
        pa.field("contrib_u_from_liquidate", pa.float64(), nullable=False),
        pa.field("contrib_u_from_withdraw", pa.float64(), nullable=False),
        pa.field("contrib_u_from_supply", pa.float64(), nullable=False),
        pa.field("contrib_u_from_interest", pa.float64(), nullable=False),
        pa.field("delta_u_pred", pa.float64(), nullable=False),
        pa.field("delta_u_real", pa.float64(), nullable=False),
        pa.field("delta_u_residual", pa.float64(), nullable=False),
    ]
    
    # D) Rate dynamics + IRM slope
    rate_dynamics_fields = [
        pa.field("delta_borrow_apr", pa.float64(), nullable=False),
        pa.field("irm_slope", pa.float64(), nullable=True),  # Can be NaN
        pa.field("irm_response_to_withdraw", pa.float64(), nullable=True),
        pa.field("irm_response_to_repay", pa.float64(), nullable=True),
        pa.field("irm_response_to_liquidate", pa.float64(), nullable=True),
    ]
    
    # Return base schema - actual table will have many more rolling window columns
    # that are dynamically generated. PyArrow will infer them from pandas.
    return pa.schema(
        base_fields + state_fields + flow_fields + contrib_fields + rate_dynamics_fields
    )
