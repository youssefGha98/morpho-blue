"""
DEPRECATED: This module has been refactored into a cleaner architecture.

The functionality has been split into:
    - domain/schemas.py: PyArrow schemas (explicit field definitions)
    - transformations/event_standardization.py: Event → Ledger conversion
    - transformations/state_enrichment.py: State computation
    - transformations/feature_engineering.py: Indicator calculation
    - validation.py: Data quality checks

Migration Guide:
    Old: from morpho_blue.indicators import build_market_ledger_raw
    New: from morpho_blue.transformations import build_market_ledger_raw
    
    Old: EventSchemas.default()
    New: Use explicit schemas from domain.schemas
    
For the new architecture, see ARCHITECTURE.md

This file is kept temporarily for backward compatibility but will be removed.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Sequence

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from morpho_blue.domain.market_events import MarketEventTables
from morpho_blue.domain.market_ledger import MarketIndicators, MarketLedger, MarketLedgerRaw

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

SECONDS_PER_YEAR = 31_536_000


# -----------------------------
# Schemas (explicit, no get())
# -----------------------------
@dataclass(frozen=True)
class EventSchema:
    event: EventName
    required_cols: tuple[str, ...]


@dataclass(frozen=True)
class EventSchemas:
    supply: EventSchema
    withdraw: EventSchema
    borrow: EventSchema
    repay: EventSchema
    liquidate: EventSchema
    accrue_interest: EventSchema
    supply_collateral: EventSchema
    withdraw_collateral: EventSchema

    @classmethod
    def default(cls) -> "EventSchemas":
        return cls(
            supply=EventSchema("Supply", ("id", "block_number", "log_index", "block_timestamp", "tx_hash", "assets")),
            withdraw=EventSchema("Withdraw", ("id", "block_number", "log_index", "block_timestamp", "tx_hash", "assets")),
            borrow=EventSchema("Borrow", ("id", "block_number", "log_index", "block_timestamp", "tx_hash", "assets")),
            repay=EventSchema("Repay", ("id", "block_number", "log_index", "block_timestamp", "tx_hash", "assets")),
            liquidate=EventSchema("Liquidate", ("id", "block_number", "log_index", "block_timestamp", "tx_hash", "repaidAssets")),
            accrue_interest=EventSchema("AccrueInterest", ("id", "block_number", "log_index", "block_timestamp", "tx_hash", "prevBorrowRate", "interest")),
            supply_collateral=EventSchema("SupplyCollateral", ("id", "block_number", "log_index", "block_timestamp", "tx_hash", "assets")),
            withdraw_collateral=EventSchema("WithdrawCollateral", ("id", "block_number", "log_index", "block_timestamp", "tx_hash", "assets")),
        )


# -----------------------------
# Validations (Arrow)
# -----------------------------
def validate_event_schema(table: pa.Table, *, schema: EventSchema) -> None:
    missing = [c for c in schema.required_cols if c not in table.column_names]
    if missing:
        raise KeyError(f"{schema.event}: missing columns {missing}. Available: {table.column_names}")


def validate_no_nulls_int(table: pa.Table, *, cols: Sequence[str], context: str) -> None:
    for col in cols:
        if col not in table.column_names:
            raise KeyError(f"{context}: missing integer column {col}")
        arr = table[col]
        # allow ChunkedArray
        null_count = arr.null_count
        if null_count and null_count > 0:
            raise ValueError(f"{context}: {col} has {null_count} nulls")
        # dtype check
        if not pa.types.is_int64(arr.type):
            raise TypeError(f"{context}: {col} must be int64; got {arr.type}")


def validate_utilization_bounds(table: pa.Table, *, col: str = "utilization_rate", context: str) -> None:
    if col not in table.column_names:
        raise KeyError(f"{context}: missing {col}")
    arr = table[col]
    # Convert to numpy for quick checks
    values = arr.to_numpy(zero_copy_only=False).astype("float64")
    if np.isnan(values).any():
        raise ValueError(f"{context}: {col} contains NaNs")
    if (values < -1e-12).any() or (values > 1 + 1e-12).any():
        raise ValueError(f"{context}: {col} out of bounds [0,1]. min={values.min()}, max={values.max()}")


# -----------------------------
# Parsing (Arrow)
# -----------------------------
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

    # We will parse via Python for mixed formats (safe + explicit).
    # This is not the fastest, but is correctness-first and localized.
    python_values = string_array.to_pylist()
    python_is_hex = is_hex.to_pylist()

    parsed_values: list[int] = []
    for value, hex_flag in zip(python_values, python_is_hex, strict=True):
        text = "0" if value is None else str(value).strip()
        if hex_flag:
            parsed_values.append(int(text, 16))
        else:
            # remove commas, parse as int
            clean = text.replace(",", "")
            if clean == "":
                parsed_values.append(0)
            else:
                # reject non-integer decimals
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

    # Cast to float64 then scale
    numeric = pc.cast(array, pa.float64())
    numeric = pc.fill_null(numeric, 0.0)
    scaled = pc.divide(numeric, pa.scalar(1e18, type=pa.float64()))
    return scaled


# -----------------------------
# Standardization (Arrow tables)
# -----------------------------
LEDGER_COLS = (
    "market_id",
    "block_number",
    "log_index",
    "block_timestamp",
    "tx_hash",
    "event_type",
    "delta_supply_assets",
    "delta_borrow_assets",
    "delta_collateral_assets",
    "borrow_rate_per_sec",
)


def _select_and_rename_market_id(table: pa.Table, *, amount_col: str) -> pa.Table:
    # We keep explicit column selection.
    selected = table.select(["id", "block_number", "log_index", "block_timestamp", "tx_hash", amount_col])
    return selected.rename_columns(["market_id", "block_number", "log_index", "block_timestamp", "tx_hash", amount_col])


def standardize_market_flow(
    *,
    event_table: pa.Table,
    schema: EventSchema,
    event_type: EventName,
    amount_col: str,
    supply_sign: int,
    borrow_sign: int,
) -> pa.Table:
    validate_event_schema(event_table, schema=schema)

    base = _select_and_rename_market_id(event_table, amount_col=amount_col)
    amount_base_units = _to_int64_base_units(base[amount_col], name=f"{event_type}.{amount_col}")

    delta_supply = pc.multiply(amount_base_units, pa.scalar(supply_sign, type=pa.int64()))
    delta_borrow = pc.multiply(amount_base_units, pa.scalar(borrow_sign, type=pa.int64()))
    delta_collateral = pa.nulls(len(base), type=pa.int64())
    delta_collateral = pc.fill_null(delta_collateral, 0)

    # borrow_rate_per_sec nulls (float64) except AccrueInterest
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
        }
    )


def standardize_collateral_flow(
    *,
    event_table: pa.Table,
    schema: EventSchema,
    event_type: EventName,
    amount_col: str,
    sign: int,
) -> pa.Table:
    validate_event_schema(event_table, schema=schema)

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
        }
    )


def standardize_accrue_interest(
    *,
    event_table: pa.Table,
    schema: EventSchema,
) -> pa.Table:
    validate_event_schema(event_table, schema=schema)

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
        }
    )


# -----------------------------
# Ledger build (Arrow)
# -----------------------------
def build_market_ledger_raw(*, events: MarketEventTables, schemas: EventSchemas) -> MarketLedgerRaw:
    standardized = [
        standardize_market_flow(
            event_table=events.supplies,
            schema=schemas.supply,
            event_type="Supply",
            amount_col="assets",
            supply_sign=+1,
            borrow_sign=0,
        ),
        standardize_market_flow(
            event_table=events.withdraws,
            schema=schemas.withdraw,
            event_type="Withdraw",
            amount_col="assets",
            supply_sign=-1,
            borrow_sign=0,
        ),
        standardize_market_flow(
            event_table=events.borrows,
            schema=schemas.borrow,
            event_type="Borrow",
            amount_col="assets",
            supply_sign=0,
            borrow_sign=+1,
        ),
        standardize_market_flow(
            event_table=events.repays,
            schema=schemas.repay,
            event_type="Repay",
            amount_col="assets",
            supply_sign=0,
            borrow_sign=-1,
        ),
        standardize_market_flow(
            event_table=events.liquidates,
            schema=schemas.liquidate,
            event_type="Liquidate",
            amount_col="repaidAssets",
            supply_sign=0,
            borrow_sign=-1,
        ),
        standardize_accrue_interest(
            event_table=events.accrue_interests,
            schema=schemas.accrue_interest,
        ),
        standardize_collateral_flow(
            event_table=events.collateral_supplies,
            schema=schemas.supply_collateral,
            event_type="SupplyCollateral",
            amount_col="assets",
            sign=+1,
        ),
        standardize_collateral_flow(
            event_table=events.collateral_withdraws,
            schema=schemas.withdraw_collateral,
            event_type="WithdrawCollateral",
            amount_col="assets",
            sign=-1,
        ),
    ]

    concatenated = pa.concat_tables(standardized, promote_options="default")

    # Sort using Arrow directly (no pandas transition needed)
    sorted_table = pc.take(
        concatenated,
        pc.sort_indices(concatenated, sort_keys=[("block_number", "ascending"), ("log_index", "ascending")])
    )

    validate_no_nulls_int(
        sorted_table,
        cols=("delta_supply_assets", "delta_borrow_assets", "delta_collateral_assets"),
        context="build_market_ledger_raw",
    )
    return MarketLedgerRaw(table=sorted_table)


# -----------------------------
# State reconstruction (DuckDB) => Arrow out
# -----------------------------
def enrich_market_ledger_with_states(
    con: duckdb.DuckDBPyConnection,
    *,
    raw_ledger: MarketLedgerRaw,
    seconds_per_year: int = SECONDS_PER_YEAR,
) -> MarketLedger:
    con.register("ledger_raw", raw_ledger.table)

    sql = """
        WITH base AS (
            SELECT
                market_id,
                block_number,
                log_index,
                block_timestamp,
                tx_hash,
                event_type,
                delta_supply_assets,
                delta_borrow_assets,
                delta_collateral_assets,
                borrow_rate_per_sec,
                ROW_NUMBER() OVER (ORDER BY block_number, log_index) AS rn
            FROM ledger_raw
        ),
        filled AS (
            SELECT
                *,
                arg_max(borrow_rate_per_sec, rn)
                    OVER (ORDER BY rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                    AS borrow_rate_per_sec_ffill
            FROM base
        )
        SELECT
            market_id,
            block_number,
            log_index,
            block_timestamp,
            tx_hash,
            event_type,

            delta_supply_assets,
            delta_borrow_assets,
            delta_collateral_assets,

            SUM(delta_supply_assets) OVER w AS total_supplied_assets,
            SUM(delta_borrow_assets) OVER w AS outstanding_borrow_assets,
            SUM(delta_collateral_assets) OVER w AS total_collateral_assets,

            borrow_rate_per_sec_ffill
        FROM filled
        WINDOW w AS (
            ORDER BY rn
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        ORDER BY block_number, log_index
    """

    state_table = con.execute(sql).fetch_arrow_table()

    # Compute derived fields using Arrow (no pandas transition)
    supply = pc.cast(state_table["total_supplied_assets"], pa.float64())
    borrow = pc.cast(state_table["outstanding_borrow_assets"], pa.float64())
    
    # utilization_rate = borrow / supply (where supply > 0, else 0)
    utilization_rate = pc.if_else(
        pc.greater(supply, pa.scalar(0.0, type=pa.float64())),
        pc.divide(borrow, supply),
        pa.scalar(0.0, type=pa.float64())
    )
    utilization_rate = pc.clip(utilization_rate, lower=0.0, upper=1.0)
    
    # borrow_apy = expm1(rate * seconds_per_year)
    rate = pc.cast(state_table["borrow_rate_per_sec_ffill"], pa.float64())
    borrow_apy_exp = pc.multiply(rate, pa.scalar(float(seconds_per_year), type=pa.float64()))
    borrow_apy = pa.array(np.expm1(borrow_apy_exp.to_numpy()), type=pa.float64())
    
    # supply_rate_per_sec = utilization_rate * borrow_rate_per_sec
    supply_rate_per_sec = pc.multiply(utilization_rate, rate)
    
    # supply_apy = expm1(supply_rate_per_sec * seconds_per_year)
    supply_apy_exp = pc.multiply(supply_rate_per_sec, pa.scalar(float(seconds_per_year), type=pa.float64()))
    supply_apy = pa.array(np.expm1(supply_apy_exp.to_numpy()), type=pa.float64())
    
    # Compute deltas (diff with first row = 0)
    util_np = utilization_rate.to_numpy()
    borrow_apy_np = borrow_apy.to_numpy()
    supply_apy_np = supply_apy.to_numpy()
    
    delta_utilization = pa.array(np.concatenate([[0.0], np.diff(util_np)]), type=pa.float64())
    delta_borrow_apy = pa.array(np.concatenate([[0.0], np.diff(borrow_apy_np)]), type=pa.float64())
    delta_supply_apy = pa.array(np.concatenate([[0.0], np.diff(supply_apy_np)]), type=pa.float64())
    
    # Build enriched table with new columns
    enriched_table = state_table.append_column(
        "utilization_rate", utilization_rate
    ).append_column(
        "borrow_apy", borrow_apy
    ).append_column(
        "supply_rate_per_sec", supply_rate_per_sec
    ).append_column(
        "supply_apy", supply_apy
    ).append_column(
        "delta_utilization", delta_utilization
    ).append_column(
        "delta_borrow_apy", delta_borrow_apy
    ).append_column(
        "delta_supply_apy", delta_supply_apy
    )
    
    # Rename borrow_rate_per_sec_ffill to borrow_rate_per_sec
    col_names = enriched_table.column_names
    col_names = ["borrow_rate_per_sec" if name == "borrow_rate_per_sec_ffill" else name for name in col_names]
    enriched_table = enriched_table.rename_columns(col_names)
    
    validate_utilization_bounds(enriched_table, context="enrich_market_ledger_with_states")

    return MarketLedger(table=enriched_table)


# -----------------------------
# Indicators (Arrow in/out; local Pandas conversion for rolling)
# -----------------------------
@dataclass(frozen=True)
class WindowSpec:
    w_5m: str = "5min"
    w_1h: str = "1h"
    w_6h: str = "6h"


def compute_market_indicators(
    *,
    ledger: MarketLedger,
    windows: WindowSpec = WindowSpec(),
    seconds_per_year: int = SECONDS_PER_YEAR,
    elasticity_eps_util: float = 1e-9,
) -> MarketIndicators:
    # Convert locally for time-based rolling. Output will be Arrow.
    ledger_df = ledger.table.to_pandas()

    # Instantaneous APR (per-sec rate * seconds/year)
    ledger_df["borrow_apr"] = ledger_df["borrow_rate_per_sec"].astype("float64") * float(seconds_per_year)

    # Time index for rolling windows
    ledger_df["_dt"] = pd.to_datetime(ledger_df["block_timestamp"].astype("int64"), unit="s", utc=True)
    ledger_df = ledger_df.set_index("_dt", drop=True)

    for window_label in (windows.w_5m, windows.w_1h, windows.w_6h):
        ledger_df[f"util_mean_{window_label}"] = ledger_df["utilization_rate"].rolling(window_label, min_periods=1).mean()
        ledger_df[f"util_std_{window_label}"] = ledger_df["utilization_rate"].rolling(window_label, min_periods=2).std().fillna(0.0)

        ledger_df[f"borrow_apr_mean_{window_label}"] = ledger_df["borrow_apr"].rolling(window_label, min_periods=1).mean()
        ledger_df[f"borrow_apr_std_{window_label}"] = ledger_df["borrow_apr"].rolling(window_label, min_periods=2).std().fillna(0.0)

    borrow_delta = ledger_df["delta_borrow_assets"].astype("int64").to_numpy()
    supply_delta = ledger_df["delta_supply_assets"].astype("int64").to_numpy()

    ledger_df["borrow_in_assets"] = np.maximum(borrow_delta, 0).astype("float64")
    ledger_df["borrow_out_assets"] = np.maximum(-borrow_delta, 0).astype("float64")
    ledger_df["supply_in_assets"] = np.maximum(supply_delta, 0).astype("float64")
    ledger_df["supply_out_assets"] = np.maximum(-supply_delta, 0).astype("float64")

    event_types = ledger_df["event_type"].astype("string").to_numpy()
    ledger_df["interest_assets"] = np.where(event_types == "AccrueInterest", borrow_delta, 0).astype("float64")

    # Intensities per window (assets per second)
    window_seconds_5m = 5 * 60
    window_seconds_1h = 60 * 60
    window_seconds_6h = 6 * 60 * 60

    for window_label, seconds in (
        (windows.w_5m, window_seconds_5m),
        (windows.w_1h, window_seconds_1h),
        (windows.w_6h, window_seconds_6h),
    ):
        denom = float(seconds)
        ledger_df[f"borrow_intensity_{window_label}"] = ledger_df["borrow_in_assets"].rolling(window_label, min_periods=1).sum() / denom
        ledger_df[f"repay_intensity_{window_label}"] = ledger_df["borrow_out_assets"].rolling(window_label, min_periods=1).sum() / denom
        ledger_df[f"supply_intensity_{window_label}"] = ledger_df["supply_in_assets"].rolling(window_label, min_periods=1).sum() / denom
        ledger_df[f"withdraw_intensity_{window_label}"] = ledger_df["supply_out_assets"].rolling(window_label, min_periods=1).sum() / denom

        mean_outstanding_borrow = ledger_df["outstanding_borrow_assets"].astype("float64").rolling(window_label, min_periods=1).mean()
        sum_interest = ledger_df["interest_assets"].rolling(window_label, min_periods=1).sum()
        ledger_df[f"interest_intensity_{window_label}"] = np.where(mean_outstanding_borrow > 0, sum_interest / mean_outstanding_borrow, 0.0)

    # Elasticity: ΔAPR / Δutil
    ledger_df["delta_borrow_apr"] = ledger_df["borrow_apr"].diff().fillna(0.0)
    ledger_df["delta_utilization"] = ledger_df["utilization_rate"].diff().fillna(0.0)

    delta_util = ledger_df["delta_utilization"].to_numpy(dtype="float64")
    delta_apr = ledger_df["delta_borrow_apr"].to_numpy(dtype="float64")

    elasticity = np.zeros_like(delta_apr, dtype="float64")
    safe = np.abs(delta_util) > float(elasticity_eps_util)
    elasticity[safe] = delta_apr[safe] / delta_util[safe]
    ledger_df["elasticity_apr_per_util"] = elasticity

    # Regime flags
    util = ledger_df["utilization_rate"].to_numpy(dtype="float64")
    ledger_df["regime_util_gt_90"] = (util > 0.90).astype("int8")
    ledger_df["regime_util_gt_95"] = (util > 0.95).astype("int8")
    ledger_df["regime_util_gt_99"] = (util > 0.99).astype("int8")

    # UI-aligned 6h average APR
    ledger_df["borrow_apr_6h_avg_ui"] = ledger_df[f"borrow_apr_mean_{windows.w_6h}"]

    ledger_df = ledger_df.reset_index(drop=True)
    out_table = pa.Table.from_pandas(ledger_df, preserve_index=False)
    validate_utilization_bounds(out_table, context="compute_market_indicators")

    return MarketIndicators(table=out_table)
