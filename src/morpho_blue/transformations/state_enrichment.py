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
    
    # Check if table is empty
    if len(raw_ledger.table) == 0:
        raise ValueError(
            "Cannot enrich empty ledger. Raw ledger has 0 rows. "
            "Check that event data was loaded correctly."
        )

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


def aggregate_ledger_to_ticks(
    con: duckdb.DuckDBPyConnection,
    *,
    ledger: MarketLedger,
    tick_seconds: int = 300,
    aggregation_level: str = "tick",
) -> MarketLedger:
    """
    Aggregate event-level ledger to tick-level or block-level.
    
    Reduces data size by aggregating events within time windows while preserving
    flow decomposition for attribution analysis.
    
    Aggregation rules:
    - Deltas: SUM of all deltas within tick/block
    - States: LAST value (from final event in tick/block)
    - Flows: Aggregated by event type (supply_in, withdraw_out, borrow_in, etc.)
    - Timestamp: Last event timestamp (for ticks with events) or tick boundary (empty ticks)
    - Empty ticks: Created with zero deltas and forward-filled states
    
    Args:
        con: DuckDB connection for SQL processing
        ledger: Enriched ledger with cumulative states
        tick_seconds: Tick duration in seconds (default 300 = 5min)
        aggregation_level: "tick" for time-based ticks, "block" for block-level aggregation
    
    Returns:
        Aggregated ledger with 1 row per tick/block
    """
    con.register("ledger_full", ledger.table)
    
    if aggregation_level == "block":
        # Block-level aggregation: group by block_number
        group_key = "block_number"
        time_bucket_expr = "block_number"
    else:
        # Tick-level aggregation: group by time bucket
        group_key = "tick"
        time_bucket_expr = f"(block_timestamp // {tick_seconds}) * {tick_seconds}"
    
    # Step 1: Aggregate events within each tick/block
    sql_aggregate = f"""
        WITH tick_events AS (
            SELECT
                {time_bucket_expr} AS {group_key},
                market_id,
                
                -- Ordering metadata (last event in tick/block)
                arg_max(block_number, struct_pack(block_number, log_index)) AS block_number,
                arg_max(log_index, struct_pack(block_number, log_index)) AS log_index,
                arg_max(block_timestamp, struct_pack(block_number, log_index)) AS block_timestamp,
                arg_max(tx_hash, struct_pack(block_number, log_index)) AS tx_hash,
                
                -- Event type placeholder (aggregated ticks combine multiple event types)
                'Aggregated' AS event_type,
                
                -- Aggregate deltas (SUM)
                SUM(delta_supply_assets) AS delta_supply_assets,
                SUM(delta_borrow_assets) AS delta_borrow_assets,
                SUM(delta_collateral_assets) AS delta_collateral_assets,
                
                -- Final states (LAST values)
                arg_max(total_supplied_assets, struct_pack(block_number, log_index)) AS total_supplied_assets,
                arg_max(outstanding_borrow_assets, struct_pack(block_number, log_index)) AS outstanding_borrow_assets,
                arg_max(total_collateral_assets, struct_pack(block_number, log_index)) AS total_collateral_assets,
                arg_max(borrow_rate_per_sec, struct_pack(block_number, log_index)) AS borrow_rate_per_sec,
                arg_max(utilization_rate, struct_pack(block_number, log_index)) AS utilization_rate,
                arg_max(borrow_apy, struct_pack(block_number, log_index)) AS borrow_apy,
                arg_max(supply_rate_per_sec, struct_pack(block_number, log_index)) AS supply_rate_per_sec,
                arg_max(supply_apy, struct_pack(block_number, log_index)) AS supply_apy,
                
                -- Aggregate flows by event type
                SUM(CASE WHEN event_type = 'Supply' AND delta_supply_assets > 0 
                    THEN delta_supply_assets ELSE 0 END) AS supply_in_assets,
                SUM(CASE WHEN event_type = 'Withdraw' AND delta_supply_assets < 0 
                    THEN ABS(delta_supply_assets) ELSE 0 END) AS withdraw_out_assets,
                SUM(CASE WHEN event_type = 'Borrow' AND delta_borrow_assets > 0 
                    THEN delta_borrow_assets ELSE 0 END) AS borrow_in_assets,
                SUM(CASE WHEN event_type = 'Repay' AND delta_borrow_assets < 0 
                    THEN ABS(delta_borrow_assets) ELSE 0 END) AS repay_out_assets,
                SUM(CASE WHEN event_type = 'Liquidate' AND delta_borrow_assets < 0 
                    THEN ABS(delta_borrow_assets) ELSE 0 END) AS liquidate_repay_assets,
                SUM(CASE WHEN event_type = 'AccrueInterest' 
                    THEN delta_borrow_assets ELSE 0 END) AS interest_assets,
                SUM(CASE WHEN event_type = 'SupplyCollateral' AND delta_collateral_assets > 0 
                    THEN delta_collateral_assets ELSE 0 END) AS collateral_in_assets,
                SUM(CASE WHEN event_type = 'WithdrawCollateral' AND delta_collateral_assets < 0 
                    THEN ABS(delta_collateral_assets) ELSE 0 END) AS collateral_out_assets,
                
                COUNT(*) AS event_count
            FROM ledger_full
            GROUP BY {group_key}, market_id
        )
        SELECT * FROM tick_events
        ORDER BY {group_key}
    """
    
    aggregated = con.execute(sql_aggregate).fetch_arrow_table()
    
    # Step 2: Fill empty ticks with zero deltas and forward-filled states
    if aggregation_level == "tick":
        # Generate complete tick range
        min_tick = aggregated[group_key][0].as_py()
        max_tick = aggregated[group_key][-1].as_py()
        
        sql_fill = f"""
            WITH tick_range AS (
                SELECT tick
                FROM range({min_tick}, {max_tick} + {tick_seconds}, {tick_seconds}) AS t(tick)
            ),
            filled AS (
                SELECT
                    tr.tick,
                    COALESCE(te.market_id, first_value(te.market_id) OVER (ORDER BY tr.tick)) AS market_id,
                    
                    -- Forward fill metadata using max() instead of last_value with IGNORE NULLS
                    COALESCE(te.block_number, 
                        max(te.block_number) OVER (ORDER BY tr.tick ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS block_number,
                    COALESCE(te.log_index,
                        max(te.log_index) OVER (ORDER BY tr.tick ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS log_index,
                    COALESCE(te.block_timestamp, tr.tick + {tick_seconds}) AS block_timestamp,
                    COALESCE(te.tx_hash,
                        max(te.tx_hash) OVER (ORDER BY tr.tick ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS tx_hash,
                    
                    -- Event type: Aggregated for all ticks
                    'Aggregated' AS event_type,
                    
                    -- Deltas: zero for empty ticks
                    COALESCE(te.delta_supply_assets, 0) AS delta_supply_assets,
                    COALESCE(te.delta_borrow_assets, 0) AS delta_borrow_assets,
                    COALESCE(te.delta_collateral_assets, 0) AS delta_collateral_assets,
                    
                    -- States: forward fill using max()
                    COALESCE(te.total_supplied_assets,
                        max(te.total_supplied_assets) OVER (ORDER BY tr.tick ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS total_supplied_assets,
                    COALESCE(te.outstanding_borrow_assets,
                        max(te.outstanding_borrow_assets) OVER (ORDER BY tr.tick ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS outstanding_borrow_assets,
                    COALESCE(te.total_collateral_assets,
                        max(te.total_collateral_assets) OVER (ORDER BY tr.tick ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS total_collateral_assets,
                    COALESCE(te.borrow_rate_per_sec,
                        max(te.borrow_rate_per_sec) OVER (ORDER BY tr.tick ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS borrow_rate_per_sec,
                    COALESCE(te.utilization_rate,
                        max(te.utilization_rate) OVER (ORDER BY tr.tick ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS utilization_rate,
                    COALESCE(te.borrow_apy,
                        max(te.borrow_apy) OVER (ORDER BY tr.tick ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS borrow_apy,
                    COALESCE(te.supply_rate_per_sec,
                        max(te.supply_rate_per_sec) OVER (ORDER BY tr.tick ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS supply_rate_per_sec,
                    COALESCE(te.supply_apy,
                        max(te.supply_apy) OVER (ORDER BY tr.tick ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS supply_apy,
                    
                    -- Flows: zero for empty ticks
                    COALESCE(te.supply_in_assets, 0) AS supply_in_assets,
                    COALESCE(te.withdraw_out_assets, 0) AS withdraw_out_assets,
                    COALESCE(te.borrow_in_assets, 0) AS borrow_in_assets,
                    COALESCE(te.repay_out_assets, 0) AS repay_out_assets,
                    COALESCE(te.liquidate_repay_assets, 0) AS liquidate_repay_assets,
                    COALESCE(te.interest_assets, 0) AS interest_assets,
                    COALESCE(te.collateral_in_assets, 0) AS collateral_in_assets,
                    COALESCE(te.collateral_out_assets, 0) AS collateral_out_assets,
                    
                    COALESCE(te.event_count, 0) AS event_count
                FROM tick_range tr
                LEFT JOIN tick_events te ON tr.tick = te.tick
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
                total_supplied_assets,
                outstanding_borrow_assets,
                total_collateral_assets,
                borrow_rate_per_sec,
                utilization_rate,
                borrow_apy,
                supply_rate_per_sec,
                supply_apy,
                supply_in_assets,
                withdraw_out_assets,
                borrow_in_assets,
                repay_out_assets,
                liquidate_repay_assets,
                interest_assets,
                collateral_in_assets,
                collateral_out_assets,
                event_count
            FROM filled
            ORDER BY tick
        """
        
        con.register("tick_events", aggregated)
        filled_table = con.execute(sql_fill).fetch_arrow_table()
    else:
        # Block-level: no empty filling needed
        filled_table = aggregated.drop(["block_number"])  # Remove duplicate group key
        filled_table = aggregated.select([col for col in aggregated.column_names if col != group_key or col == "block_number"])
    
    # Compute delta metrics (diff of rates/utilization)
    util_np = filled_table["utilization_rate"].to_numpy()
    borrow_apy_np = filled_table["borrow_apy"].to_numpy()
    supply_apy_np = filled_table["supply_apy"].to_numpy()
    
    delta_utilization = pa.array(np.concatenate([[0.0], np.diff(util_np)]), type=pa.float64())
    delta_borrow_apy = pa.array(np.concatenate([[0.0], np.diff(borrow_apy_np)]), type=pa.float64())
    delta_supply_apy = pa.array(np.concatenate([[0.0], np.diff(supply_apy_np)]), type=pa.float64())
    
    filled_table = filled_table.append_column("delta_utilization", delta_utilization)
    filled_table = filled_table.append_column("delta_borrow_apy", delta_borrow_apy)
    filled_table = filled_table.append_column("delta_supply_apy", delta_supply_apy)
    
    return MarketLedger(table=filled_table)
