"""
Feature engineering: Compute advanced indicators from enriched ledger.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
import pyarrow as pa

from morpho_blue.domain.market_ledger import MarketIndicators, MarketLedger
from morpho_blue.domain.schemas import market_indicators_schema

SECONDS_PER_YEAR = 31_536_000


@dataclass(frozen=True)
class WindowSpec:
    """Rolling window specifications."""
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
    """
    Compute advanced indicators with rolling windows and feature engineering.
    
    Adds:
        - Instantaneous APR
        - Flow decomposition (in/out flows)
        - Rolling statistics (utilization, APR)
        - Intensity metrics (per-second rates)
        - Elasticity measures
        - Regime indicators
    
    Args:
        ledger: Enriched ledger with states
        windows: Window specifications for rolling metrics
        seconds_per_year: Seconds per year for APR calculation
        elasticity_eps_util: Epsilon for elasticity calculation
    
    Returns:
        Full indicator dataset
    """
    ledger_df = ledger.table.to_pandas()

    # Instantaneous APR
    ledger_df["borrow_apr"] = ledger_df["borrow_rate_per_sec"].astype("float64") * float(seconds_per_year)

    # Time index for rolling windows
    ledger_df["_dt"] = pd.to_datetime(ledger_df["block_timestamp"].astype("int64"), unit="s", utc=True)
    ledger_df = ledger_df.set_index("_dt", drop=True)

    # Rolling window statistics
    for window_label in (windows.w_5m, windows.w_1h, windows.w_6h):
        ledger_df[f"util_mean_{window_label}"] = ledger_df["utilization_rate"].rolling(window_label, min_periods=1).mean()
        ledger_df[f"util_std_{window_label}"] = ledger_df["utilization_rate"].rolling(window_label, min_periods=2).std().fillna(0.0)

        ledger_df[f"borrow_apr_mean_{window_label}"] = ledger_df["borrow_apr"].rolling(window_label, min_periods=1).mean()
        ledger_df[f"borrow_apr_std_{window_label}"] = ledger_df["borrow_apr"].rolling(window_label, min_periods=2).std().fillna(0.0)

    # Flow decomposition
    borrow_delta = ledger_df["delta_borrow_assets"].astype("int64").to_numpy()
    supply_delta = ledger_df["delta_supply_assets"].astype("int64").to_numpy()

    ledger_df["borrow_in_assets"] = np.maximum(borrow_delta, 0).astype("float64")
    ledger_df["borrow_out_assets"] = np.maximum(-borrow_delta, 0).astype("float64")
    ledger_df["supply_in_assets"] = np.maximum(supply_delta, 0).astype("float64")
    ledger_df["supply_out_assets"] = np.maximum(-supply_delta, 0).astype("float64")

    event_types = ledger_df["event_type"].astype("string").to_numpy()
    ledger_df["interest_assets"] = np.where(event_types == "AccrueInterest", borrow_delta, 0).astype("float64")

    # Intensity metrics (assets per second)
    window_seconds = {
        windows.w_5m: 5 * 60,
        windows.w_1h: 60 * 60,
        windows.w_6h: 6 * 60 * 60,
    }

    for window_label, seconds in window_seconds.items():
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

    # Regime indicators
    util = ledger_df["utilization_rate"].to_numpy(dtype="float64")
    ledger_df["regime_util_gt_90"] = (util > 0.90).astype("int8")
    ledger_df["regime_util_gt_95"] = (util > 0.95).astype("int8")
    ledger_df["regime_util_gt_99"] = (util > 0.99).astype("int8")

    # UI-aligned metrics
    ledger_df["borrow_apr_6h_avg_ui"] = ledger_df[f"borrow_apr_mean_{windows.w_6h}"]

    # Reset index and convert to Arrow
    ledger_df = ledger_df.reset_index(drop=True)
    out_table = pa.Table.from_pandas(ledger_df, preserve_index=False, schema=market_indicators_schema())

    return MarketIndicators(table=out_table)
