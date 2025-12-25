"""
State enrichment: Add cumulative states and derived metrics to raw ledger.
"""
from __future__ import annotations

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from morpho_blue.domain.market_ledger import MarketLedger, MarketLedgerRaw
from morpho_blue.domain.schemas import market_ledger_schema

SECONDS_PER_YEAR = 31_536_000


def enrich_market_ledger_with_states(
    con: duckdb.DuckDBPyConnection,
    *,
    raw_ledger: MarketLedgerRaw,
    seconds_per_year: int = SECONDS_PER_YEAR,
) -> MarketLedger:
    """
    Enrich raw ledger with cumulative states and computed metrics.
    
    Adds:
        - Running totals (total_supplied_assets, outstanding_borrow_assets, total_collateral_assets)
        - Forward-filled borrow_rate_per_sec
        - Utilization rate
        - APYs (borrow and supply)
        - Delta metrics
    
    Args:
        con: DuckDB connection for SQL processing
        raw_ledger: Raw ledger with deltas
        seconds_per_year: Seconds per year for APY calculation
    
    Returns:
        Enriched ledger with all state metrics
    """
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

    # Compute derived metrics using Arrow (no pandas transition)
    supply = pc.cast(state_table["total_supplied_assets"], pa.float64())
    borrow = pc.cast(state_table["outstanding_borrow_assets"], pa.float64())
    
    # utilization_rate = borrow / supply (where supply > 0, else 0)
    utilization_rate = pc.if_else(
        pc.greater(supply, pa.scalar(0.0, type=pa.float64())),
        pc.divide(borrow, supply),
        pa.scalar(0.0, type=pa.float64())
    )
    # Clip using numpy
    util_np = utilization_rate.to_numpy()
    utilization_rate = pa.array(np.clip(util_np, 0.0, 1.0), type=pa.float64())
    
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
    
    # Cast to explicit schema
    enriched_table = enriched_table.cast(market_ledger_schema())

    return MarketLedger(table=enriched_table)
