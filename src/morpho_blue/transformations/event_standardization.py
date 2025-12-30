"""
Event standardization: Convert raw events to unified ledger format.
Separates data transformation logic from domain models.
"""
from __future__ import annotations

from typing import Literal

import pyarrow as pa
import pyarrow.compute as pc

from morpho_blue.domain.market_events import MarketEventTables
from morpho_blue.domain.market_ledger import MarketLedgerRaw
from morpho_blue.domain.schemas import market_ledger_raw_schema

EventName = Literal[
    "AccrueInterest",
    "Borrow",
    "Liquidate",
    "Repay",
    "Supply",
    "SupplyCollateral",
    "Withdraw",
    "WithdrawCollateral",
]


# ===========================
# Parsing Utilities
# ===========================

def _to_int64_base_units(array: pa.ChunkedArray | pa.Array, *, name: str) -> pa.Array:
    """
    Parse to int64 base units. Supports:
      - int64 already
      - decimal strings
      - hex strings "0x..."
    Nulls -> 0 to match your current behavior.
    """
    if isinstance(array, pa.ChunkedArray):
        array = array.combine_chunks()

    if pa.types.is_int64(array.type):
        # fill null with 0 for consistency
        if array.null_count:
            return pc.fill_null(array, 0).cast(pa.int64())
        return array

    # Convert to strings for parsing
    string_array = pc.cast(array, pa.string())
    string_array = pc.utf8_trim_whitespace(string_array)
    string_array = pc.fill_null(string_array, "0")

    # Identify hex rows
    starts_0x = pc.starts_with(string_array, "0x")
    starts_0X = pc.starts_with(string_array, "0X")
    is_hex = pc.or_(starts_0x, starts_0X)

    # Parse via Python for mixed formats
    python_values = string_array.to_pylist()
    python_is_hex = is_hex.to_pylist()

    parsed_values: list[int] = []
    for value, hex_flag in zip(python_values, python_is_hex, strict=True):
        text = "0" if value is None else str(value).strip()
        if hex_flag:
            parsed_values.append(int(text, 16))
        else:
            clean = text.replace(",", "")
            if clean == "":
                parsed_values.append(0)
            else:
                if "." in clean:
                    raise ValueError(f"{name}: non-integer value {text!r}")
                parsed_values.append(int(clean))

    return pa.array(parsed_values, type=pa.int64())


def _rate_wad_per_sec_to_float64(array: pa.ChunkedArray | pa.Array, *, name: str) -> pa.Array:
    """
    prevBorrowRate (WAD) => float per-second: / 1e18
    """
    if isinstance(array, pa.ChunkedArray):
        array = array.combine_chunks()

    numeric = pc.cast(array, pa.float64())
    numeric = pc.fill_null(numeric, 0.0)
    scaled = pc.divide(numeric, pa.scalar(1e18, type=pa.float64()))
    return scaled


def _select_and_rename_market_id(table: pa.Table, *, amount_col: str) -> pa.Table:
    """Extract base columns and rename 'id' to 'market_id'."""
    selected = table.select(["id", "block_number", "log_index", "block_timestamp", "tx_hash", amount_col])
    return selected.rename_columns(["market_id", "block_number", "log_index", "block_timestamp", "tx_hash", amount_col])


# ===========================
# Standardization Functions
# ===========================

def standardize_market_flow(
    *,
    event_table: pa.Table,
    event_type: EventName,
    amount_col: str,
    supply_sign: int,
    borrow_sign: int,
) -> pa.Table:
    """
    Standardize market flow events (Supply, Withdraw, Borrow, Repay, Liquidate).
    
    Args:
        event_table: Raw event data
        event_type: Type of event
        amount_col: Column name containing amount
        supply_sign: +1 for supply, -1 for withdraw, 0 otherwise
        borrow_sign: +1 for borrow, -1 for repay/liquidate, 0 otherwise
    
    Returns:
        Standardized table matching market_ledger_raw_schema
    """
    base = _select_and_rename_market_id(event_table, amount_col=amount_col)
    amount_base_units = _to_int64_base_units(base[amount_col], name=f"{event_type}.{amount_col}")

    delta_supply = pc.multiply(amount_base_units, pa.scalar(supply_sign, type=pa.int64()))
    delta_borrow = pc.multiply(amount_base_units, pa.scalar(borrow_sign, type=pa.int64()))
    delta_collateral = pa.nulls(len(base), type=pa.int64())
    delta_collateral = pc.fill_null(delta_collateral, 0)

    rate_null = pa.nulls(len(base), type=pa.float64())

    return pa.table(
        {
            "market_id": base["market_id"],
            "block_number": base["block_number"],
            "log_index": base["log_index"],
            "block_timestamp": base["block_timestamp"],
            "tx_hash": base["tx_hash"],
            "event_type": pa.array([event_type] * len(base), type=pa.string()),
            "delta_supply_assets": pc.cast(delta_supply, pa.int64()),
            "delta_borrow_assets": pc.cast(delta_borrow, pa.int64()),
            "delta_collateral_assets": pc.cast(delta_collateral, pa.int64()),
            "borrow_rate_per_sec": rate_null,
        },
        schema=market_ledger_raw_schema(),
    )


def standardize_collateral_flow(
    *,
    event_table: pa.Table,
    event_type: EventName,
    amount_col: str,
    sign: int,
) -> pa.Table:
    """
    Standardize collateral flow events (SupplyCollateral, WithdrawCollateral).
    
    Args:
        event_table: Raw event data
        event_type: Type of event
        amount_col: Column name containing amount
        sign: +1 for supply, -1 for withdraw
    
    Returns:
        Standardized table matching market_ledger_raw_schema
    """
    base = _select_and_rename_market_id(event_table, amount_col=amount_col)
    amount_base_units = _to_int64_base_units(base[amount_col], name=f"{event_type}.{amount_col}")

    delta_collateral = pc.multiply(amount_base_units, pa.scalar(sign, type=pa.int64()))
    rate_null = pa.nulls(len(base), type=pa.float64())

    return pa.table(
        {
            "market_id": base["market_id"],
            "block_number": base["block_number"],
            "log_index": base["log_index"],
            "block_timestamp": base["block_timestamp"],
            "tx_hash": base["tx_hash"],
            "event_type": pa.array([event_type] * len(base), type=pa.string()),
            "delta_supply_assets": pa.array([0] * len(base), type=pa.int64()),
            "delta_borrow_assets": pa.array([0] * len(base), type=pa.int64()),
            "delta_collateral_assets": pc.cast(delta_collateral, pa.int64()),
            "borrow_rate_per_sec": rate_null,
        },
        schema=market_ledger_raw_schema(),
    )


def standardize_accrue_interest(
    *,
    event_table: pa.Table,
) -> pa.Table:
    """
    Standardize AccrueInterest events.
    
    Args:
        event_table: Raw event data
    
    Returns:
        Standardized table matching market_ledger_raw_schema
    """
    selected = event_table.select(
        ["id", "block_number", "log_index", "block_timestamp", "tx_hash", "prevBorrowRate", "interest"]
    ).rename_columns(["market_id", "block_number", "log_index", "block_timestamp", "tx_hash", "prevBorrowRate", "interest"])

    interest_base_units = _to_int64_base_units(selected["interest"], name="AccrueInterest.interest")
    borrow_rate_per_sec = _rate_wad_per_sec_to_float64(selected["prevBorrowRate"], name="AccrueInterest.prevBorrowRate")

    # feeShare=0 correctness patch: interest accrues to BOTH totals
    return pa.table(
        {
            "market_id": selected["market_id"],
            "block_number": selected["block_number"],
            "log_index": selected["log_index"],
            "block_timestamp": selected["block_timestamp"],
            "tx_hash": selected["tx_hash"],
            "event_type": pa.array(["AccrueInterest"] * len(selected), type=pa.string()),
            "delta_supply_assets": pc.cast(interest_base_units, pa.int64()),
            "delta_borrow_assets": pc.cast(interest_base_units, pa.int64()),
            "delta_collateral_assets": pa.array([0] * len(selected), type=pa.int64()),
            "borrow_rate_per_sec": borrow_rate_per_sec,
        },
        schema=market_ledger_raw_schema(),
    )


# ===========================
# Ledger Builder
# ===========================

def build_market_ledger_raw(*, events: MarketEventTables) -> MarketLedgerRaw:
    """
    Build unified ledger from all market events.
    
    Args:
        events: All market event tables
    
    Returns:
        Standardized and sorted ledger
    """
    standardized = [
        standardize_market_flow(
            event_table=events.supplies,
            event_type="Supply",
            amount_col="assets",
            supply_sign=+1,
            borrow_sign=0,
        ),
        standardize_market_flow(
            event_table=events.withdraws,
            event_type="Withdraw",
            amount_col="assets",
            supply_sign=-1,
            borrow_sign=0,
        ),
        standardize_market_flow(
            event_table=events.borrows,
            event_type="Borrow",
            amount_col="assets",
            supply_sign=0,
            borrow_sign=+1,
        ),
        standardize_market_flow(
            event_table=events.repays,
            event_type="Repay",
            amount_col="assets",
            supply_sign=0,
            borrow_sign=-1,
        ),
        standardize_market_flow(
            event_table=events.liquidates,
            event_type="Liquidate",
            amount_col="repaidAssets",
            supply_sign=0,
            borrow_sign=-1,
        ),
        standardize_accrue_interest(
            event_table=events.accrue_interests,
        ),
        standardize_collateral_flow(
            event_table=events.collateral_supplies,
            event_type="SupplyCollateral",
            amount_col="assets",
            sign=+1,
        ),
        standardize_collateral_flow(
            event_table=events.collateral_withdraws,
            event_type="WithdrawCollateral",
            amount_col="assets",
            sign=-1,
        ),
    ]

    # Filter out empty tables before concatenation
    non_empty_tables = [t for t in standardized if len(t) > 0]
    
    if not non_empty_tables:
        # Create an empty table with the correct schema
        schema = market_ledger_raw_schema()
        empty_data = {field.name: pa.array([], type=field.type) for field in schema}
        return MarketLedgerRaw(table=pa.table(empty_data, schema=schema))
    
    concatenated = pa.concat_tables(non_empty_tables, promote_options="default")

    # Sort using Arrow directly (no pandas transition needed)
    sorted_table = pc.take(
        concatenated,
        pc.sort_indices(concatenated, sort_keys=[("block_number", "ascending"), ("log_index", "ascending")])
    )
    
    # Ensure schema compliance
    sorted_table = sorted_table.cast(market_ledger_raw_schema())

    return MarketLedgerRaw(table=sorted_table)
