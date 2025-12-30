"""
Attribution layer: Decompose utilization dynamics into flow contributions.

Implements comprehensive attribution of utilization changes to:
- Borrow/Repay/Liquidate principal flows
- Supply/Withdraw flows  
- Interest accrual
- IRM slope diagnostics
- Rolling window aggregations
- Accounting integrity residuals
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np
import pandas as pd
import pyarrow as pa

from morpho_blue.domain.market_ledger import MarketLedger

SECONDS_PER_YEAR = 31_536_000
EPS_U = 1e-9  # Epsilon for utilization delta safety checks
EPS_CONTRIB = 1e-12  # Epsilon for contribution denominators


def preprocess_ledger_for_rolling_windows(
    *,
    df: pd.DataFrame,
    tick_seconds: int = 300,  # 5 minutes default
) -> pd.DataFrame:
    """
    Preprocess ledger to keep only first and last rows within each time tick.
    
    This dramatically reduces dataset size for rolling window calculations while
    preserving tick boundaries needed for accurate window statistics.
    
    For a 5min tick analysis, we only need:
    - Last row before each tick (ending state)
    - First row after each tick (starting state of next tick)
    
    This can reduce dataset size by 90%+ for high-frequency event data.
    
    Args:
        df: Ledger dataframe with block_timestamp column
        tick_seconds: Tick size in seconds (default 300 = 5min)
    
    Returns:
        Reduced dataframe with first/last rows per tick
    """
    if len(df) == 0:
        return df
    
    # Create tick boundaries
    df = df.copy()
    df["_tick"] = (df["block_timestamp"].astype("int64") // tick_seconds) * tick_seconds
    
    # Group by tick and keep first and last row
    # We use groupby with idx to preserve all columns
    groups = df.groupby("_tick", sort=False)
    
    # Get indices of first and last rows per tick
    first_idx = groups.head(1).index
    last_idx = groups.tail(1).index
    
    # Combine indices (use union to avoid duplicates if tick has only 1 row)
    keep_idx = first_idx.union(last_idx)
    
    # Filter dataframe
    result = df.loc[keep_idx].sort_index()
    result = result.drop(columns=["_tick"])
        
    return result


@dataclass(frozen=True)
class AttributionWindowSpec:
    """Rolling window specifications for attribution features."""
    atomic: str = "atomic"  # per-row
    w_5m: str = "5min"
    w_1h: str = "1h"
    w_6h: str = "6h"
    w_24h: str = "24h"
    w_7d: str = "7d"
    w_30d: str = "30d"
    w_90d: str = "90d"
    w_365d: str = "365d"
    
    # Optional: specify which windows to actually compute (None = all)
    compute_windows: list[str] | None = None
    
    def all_time_windows(self) -> list[str]:
        """Return all time-based window labels."""
        all_windows = [
            self.w_5m,
            self.w_1h,
            self.w_6h,
            self.w_24h,
            self.w_7d,
            self.w_30d,
            self.w_90d,
            self.w_365d,
        ]
        # Return only specified windows if compute_windows is set
        if self.compute_windows is not None:
            return [w for w in all_windows if w in self.compute_windows]
        return all_windows
    
    def window_seconds(self) -> dict[str, int]:
        """Return window duration in seconds for each window."""
        return {
            self.w_5m: 5 * 60,
            self.w_1h: 60 * 60,
            self.w_6h: 6 * 60 * 60,
            self.w_24h: 24 * 60 * 60,
            self.w_7d: 7 * 24 * 60 * 60,
            self.w_30d: 30 * 24 * 60 * 60,
            self.w_90d: 90 * 24 * 60 * 60,
            self.w_365d: 365 * 24 * 60 * 60,
        }


def compute_attribution_features(
    *,
    ledger: MarketLedger,
    windows: AttributionWindowSpec = AttributionWindowSpec(),
    compute_windows: list[str],
    seconds_per_year: int = SECONDS_PER_YEAR,
    eps_u: float = EPS_U,
    eps_contrib: float = EPS_CONTRIB,
    preprocess_ticks: bool = True,
    tick_seconds: int = 300,
    batch_size: int | None = None,
) -> pa.Table:
    """
    Compute full attribution feature table from enriched ledger.
    
    Adds:
        A) State columns (levels): buffer, headroom, APR
        B) Flow decomposition: borrow_in, repay_out, liquidate_repay, supply_in, withdraw_out, interest
        C) Utilization contributions: attribution of Δu to each flow type
        D) Rate dynamics: delta_borrow_apr, irm_slope, factor-conditioned responses
        E) Rolling windows: means, maxes, intensities, volatilities, attribution aggregates
        F) Validation residuals: delta_u_residual for integrity checks
    
    Args:
        ledger: Enriched ledger with cumulative states
        windows: Window specifications for rolling metrics
        compute_windows: Optional list of specific windows to compute (e.g., ['5min', '1h']). 
                        If None, uses windows.compute_windows or all windows.
        seconds_per_year: Seconds per year for APR calculation
        eps_u: Epsilon for utilization delta safety
        eps_contrib: Epsilon for contribution denominators
        preprocess_ticks: If True, reduce dataset by keeping only first/last rows per tick
        tick_seconds: Tick size in seconds for preprocessing (default 300 = 5min)
        batch_size: If set, process rolling windows in batches to reduce memory usage
    
    Returns:
        PyArrow table with full attribution features
    """
    # Convert to Pandas for time-series operations
    df = ledger.to_pandas()
    
    # OPTIMIZATION: Preprocess to reduce dataset size for rolling windows
    if preprocess_ticks and len(df) > 1000:  # Only worth it for larger datasets
        df = preprocess_ledger_for_rolling_windows(df=df, tick_seconds=tick_seconds)
    
    # Ensure integer types are preserved
    int_cols = ["delta_supply_assets", "delta_borrow_assets", "delta_collateral_assets",
                "total_supplied_assets", "outstanding_borrow_assets", "total_collateral_assets"]
    for col in int_cols:
        if col in df.columns:
            df[col] = df[col].astype("int64")
    
    # Set time index for rolling operations
    df["_dt"] = pd.to_datetime(df["block_timestamp"].astype("int64"), unit="s", utc=True)
    df = df.set_index("_dt", drop=True)
    
    # ==========================================
    # A) STATE COLUMNS (LEVELS)
    # ==========================================
    
    # Compute borrow_apr (instantaneous)
    df["borrow_apr"] = df["borrow_rate_per_sec"].astype("float64") * float(seconds_per_year)
    
    # Buffer = 1 - utilization
    df["buffer"] = (1.0 - df["utilization_rate"].astype("float64")).clip(lower=0.0)
    
    # Headroom metrics (int64)
    total_supply = df["total_supplied_assets"].astype("int64")
    outstanding_borrow = df["outstanding_borrow_assets"].astype("int64")
    df["headroom_u90_assets"] = (0.90 * total_supply - outstanding_borrow).astype("int64")
    df["headroom_u95_assets"] = (0.95 * total_supply - outstanding_borrow).astype("int64")
    
    # ==========================================
    # B) FLOW DECOMPOSITION (PER-STEP)
    # ==========================================
    
    event_type = df["event_type"].astype("string").to_numpy()
    delta_borrow = df["delta_borrow_assets"].astype("int64").to_numpy()
    delta_supply = df["delta_supply_assets"].astype("int64").to_numpy()
    
    # Borrow-side principal flows (float64 for later calculations)
    df["borrow_in_assets"] = np.where(
        event_type == "Borrow",
        np.maximum(delta_borrow, 0),
        0
    ).astype("float64")
    
    df["repay_out_assets"] = np.where(
        event_type == "Repay",
        np.abs(np.minimum(delta_borrow, 0)),
        0
    ).astype("float64")
    
    # Liquidate: delta_borrow_assets is negative (repaidAssets with sign=-1)
    df["liquidate_repay_assets"] = np.where(
        event_type == "Liquidate",
        np.abs(np.minimum(delta_borrow, 0)),
        0
    ).astype("float64")
    
    # Supply-side flows
    df["supply_in_assets"] = np.where(
        event_type == "Supply",
        np.maximum(delta_supply, 0),
        0
    ).astype("float64")
    
    df["withdraw_out_assets"] = np.where(
        event_type == "Withdraw",
        np.abs(np.minimum(delta_supply, 0)),
        0
    ).astype("float64")
    
    # Interest accrual (feeShare=0 → interest goes to both totals)
    df["interest_assets"] = np.where(
        event_type == "AccrueInterest",
        delta_borrow,  # For feeShare=0, delta_borrow_assets == interest
        0
    ).astype("float64")
    
    # Derived net flows
    df["net_borrow_principal_assets"] = (
        df["borrow_in_assets"] - df["repay_out_assets"] - df["liquidate_repay_assets"]
    )
    df["net_supply_assets"] = df["supply_in_assets"] - df["withdraw_out_assets"]
    
    # ==========================================
    # C) UTILIZATION CONTRIBUTION TERMS
    # ==========================================
    
    # Previous state values (shift by 1)
    total_supply_prev = df["total_supplied_assets"].shift(1).fillna(0).astype("float64")
    util_prev = df["utilization_rate"].shift(1).fillna(0).astype("float64")
    
    # Safe denominator (avoid division by zero)
    total_supply_prev_safe = np.maximum(total_supply_prev.to_numpy(), 1.0)
    
    # Contribution from borrow (increases utilization)
    df["contrib_u_from_borrow"] = df["borrow_in_assets"] / total_supply_prev_safe
    
    # Contribution from repay (decreases utilization)
    df["contrib_u_from_repay"] = -df["repay_out_assets"] / total_supply_prev_safe
    
    # Contribution from liquidate (decreases utilization)
    df["contrib_u_from_liquidate"] = -df["liquidate_repay_assets"] / total_supply_prev_safe
    
    # Contribution from withdraw (increases utilization via denominator effect)
    # Δu ≈ -B_prev / S_prev^2 * ΔS = u_prev * (ΔS / S_prev)
    util_prev_arr = util_prev.to_numpy()
    df["contrib_u_from_withdraw"] = util_prev_arr * (df["withdraw_out_assets"] / total_supply_prev_safe)
    
    # Contribution from supply (decreases utilization via denominator effect)
    df["contrib_u_from_supply"] = -util_prev_arr * (df["supply_in_assets"] / total_supply_prev_safe)
    
    # Contribution from interest (complex: affects both numerator and denominator)
    # Under feeShare=0: interest adds equally to both B and S
    # Δu ≈ (I/S) - u_prev*(I/S) = (I/S)*(1 - u_prev)
    df["contrib_u_from_interest"] = (
        (df["interest_assets"] / total_supply_prev_safe) * (1.0 - util_prev_arr)
    )
    
    # Predicted delta utilization (sum of all contributions)
    df["delta_u_pred"] = (
        df["contrib_u_from_borrow"] +
        df["contrib_u_from_repay"] +
        df["contrib_u_from_liquidate"] +
        df["contrib_u_from_withdraw"] +
        df["contrib_u_from_supply"] +
        df["contrib_u_from_interest"]
    )
    
    # Real delta utilization (from actual data)
    df["delta_u_real"] = df["utilization_rate"].diff().fillna(0.0)
    
    # Residual (integrity check)
    df["delta_u_residual"] = df["delta_u_real"] - df["delta_u_pred"]
    
    # ==========================================
    # D) RATE DYNAMICS + IRM SLOPE
    # ==========================================
    
    # Delta borrow APR
    df["delta_borrow_apr"] = df["borrow_apr"].diff().fillna(0.0)
    
    # IRM slope: ΔAPR / Δu (only when |Δu| > eps)
    delta_u_real_arr = df["delta_u_real"].to_numpy()
    delta_apr_arr = df["delta_borrow_apr"].to_numpy()
    
    irm_slope = np.full_like(delta_apr_arr, np.nan, dtype="float64")
    valid_mask = np.abs(delta_u_real_arr) > eps_u
    irm_slope[valid_mask] = delta_apr_arr[valid_mask] / delta_u_real_arr[valid_mask]
    df["irm_slope"] = irm_slope
    
    # Factor-conditioned IRM responses (elasticity to each contributor)
    # Response to withdraw
    contrib_withdraw = df["contrib_u_from_withdraw"].to_numpy()
    irm_response_withdraw = np.full_like(delta_apr_arr, np.nan, dtype="float64")
    valid_withdraw = np.abs(contrib_withdraw) > eps_contrib
    irm_response_withdraw[valid_withdraw] = delta_apr_arr[valid_withdraw] / contrib_withdraw[valid_withdraw]
    df["irm_response_to_withdraw"] = irm_response_withdraw
    
    # Response to repay
    contrib_repay = df["contrib_u_from_repay"].to_numpy()
    irm_response_repay = np.full_like(delta_apr_arr, np.nan, dtype="float64")
    valid_repay = np.abs(contrib_repay) > eps_contrib
    irm_response_repay[valid_repay] = delta_apr_arr[valid_repay] / contrib_repay[valid_repay]
    df["irm_response_to_repay"] = irm_response_repay
    
    # Response to liquidate
    contrib_liquidate = df["contrib_u_from_liquidate"].to_numpy()
    irm_response_liquidate = np.full_like(delta_apr_arr, np.nan, dtype="float64")
    valid_liquidate = np.abs(contrib_liquidate) > eps_contrib
    irm_response_liquidate[valid_liquidate] = delta_apr_arr[valid_liquidate] / contrib_liquidate[valid_liquidate]
    df["irm_response_to_liquidate"] = irm_response_liquidate
    
    # ==========================================
    # E) ROLLING WINDOWS (WITH BATCH PROCESSING)
    # ==========================================
    
    def process_window_batch(df_batch: pd.DataFrame, window_labels: list[str]) -> pd.DataFrame:
        """Process a batch of rolling windows to reduce memory footprint."""
        for window_label in window_labels:
            # Level statistics
            df_batch[f"util_mean_{window_label}"] = df_batch["utilization_rate"].rolling(window_label, min_periods=1).mean()
            df_batch[f"util_max_{window_label}"] = df_batch["utilization_rate"].rolling(window_label, min_periods=1).max()
            df_batch[f"util_std_{window_label}"] = df_batch["utilization_rate"].rolling(window_label, min_periods=2).std().fillna(0.0)
            
            df_batch[f"buffer_min_{window_label}"] = df_batch["buffer"].rolling(window_label, min_periods=1).min()
            
            df_batch[f"borrow_apr_mean_{window_label}"] = df_batch["borrow_apr"].rolling(window_label, min_periods=1).mean()
            df_batch[f"borrow_apr_max_{window_label}"] = df_batch["borrow_apr"].rolling(window_label, min_periods=1).max()
            df_batch[f"borrow_apr_std_{window_label}"] = df_batch["borrow_apr"].rolling(window_label, min_periods=2).std().fillna(0.0)
            
            # Flow intensities (assets per second)
            seconds_w = float(windows.window_seconds()[window_label])
            df_batch[f"borrow_intensity_{window_label}"] = df_batch["borrow_in_assets"].rolling(window_label, min_periods=1).sum() / seconds_w
            df_batch[f"repay_intensity_{window_label}"] = df_batch["repay_out_assets"].rolling(window_label, min_periods=1).sum() / seconds_w
            df_batch[f"liquidate_intensity_{window_label}"] = df_batch["liquidate_repay_assets"].rolling(window_label, min_periods=1).sum() / seconds_w
            df_batch[f"withdraw_intensity_{window_label}"] = df_batch["withdraw_out_assets"].rolling(window_label, min_periods=1).sum() / seconds_w
            df_batch[f"supply_intensity_{window_label}"] = df_batch["supply_in_assets"].rolling(window_label, min_periods=1).sum() / seconds_w
            df_batch[f"interest_intensity_{window_label}"] = df_batch["interest_assets"].rolling(window_label, min_periods=1).sum() / seconds_w
            
            # Scale-free intensity (normalized by mean supply)
            mean_supply_window = df_batch["total_supplied_assets"].astype("float64").rolling(window_label, min_periods=1).mean()
            mean_supply_safe = np.maximum(mean_supply_window.to_numpy(), 1.0)
            
            df_batch[f"borrow_intensity_norm_supply_{window_label}"] = (
                df_batch[f"borrow_intensity_{window_label}"] / mean_supply_safe
            )
            df_batch[f"withdraw_intensity_norm_supply_{window_label}"] = (
                df_batch[f"withdraw_intensity_{window_label}"] / mean_supply_safe
            )
            df_batch[f"repay_intensity_norm_supply_{window_label}"] = (
                df_batch[f"repay_intensity_{window_label}"] / mean_supply_safe
            )
            
            # Attribution aggregation (utilization contribution sums)
            df_batch[f"contrib_withdraw_sum_{window_label}"] = df_batch["contrib_u_from_withdraw"].rolling(window_label, min_periods=1).sum()
            df_batch[f"contrib_repay_sum_{window_label}"] = df_batch["contrib_u_from_repay"].rolling(window_label, min_periods=1).sum()
            df_batch[f"contrib_liquidate_sum_{window_label}"] = df_batch["contrib_u_from_liquidate"].rolling(window_label, min_periods=1).sum()
            df_batch[f"contrib_borrow_sum_{window_label}"] = df_batch["contrib_u_from_borrow"].rolling(window_label, min_periods=1).sum()
            df_batch[f"contrib_supply_sum_{window_label}"] = df_batch["contrib_u_from_supply"].rolling(window_label, min_periods=1).sum()
            
            # Absolute contribution sums (activity magnitude)
            df_batch[f"contrib_withdraw_abs_sum_{window_label}"] = df_batch["contrib_u_from_withdraw"].abs().rolling(window_label, min_periods=1).sum()
            df_batch[f"contrib_repay_abs_sum_{window_label}"] = df_batch["contrib_u_from_repay"].abs().rolling(window_label, min_periods=1).sum()
            df_batch[f"contrib_liquidate_abs_sum_{window_label}"] = df_batch["contrib_u_from_liquidate"].abs().rolling(window_label, min_periods=1).sum()
            
            # Volatility / fragility
            df_batch[f"delta_u_std_{window_label}"] = df_batch["delta_u_real"].rolling(window_label, min_periods=2).std().fillna(0.0)
            
            # IRM slope volatility (winsorized to avoid outliers)
            irm_slope_winsorized = df_batch["irm_slope"].clip(lower=-1000, upper=1000).fillna(0.0)
            df_batch[f"irm_slope_std_{window_label}"] = irm_slope_winsorized.rolling(window_label, min_periods=2).std().fillna(0.0)
        
        return df_batch
    
    # Process windows in batches if batch_size is specified
    if batch_size and len(compute_windows) > batch_size:
        print(f"Processing {len(compute_windows)} windows in batches of {batch_size}...")
        for i in range(0, len(compute_windows), batch_size):
            batch_windows = compute_windows[i:i + batch_size]
            print(f"  Batch {i//batch_size + 1}: {batch_windows}")
            df = process_window_batch(df, batch_windows)
    else:
        # Process all windows at once
        df = process_window_batch(df, compute_windows)
    
    # Compute volatility attribution shares (advanced)
    for window_label in compute_windows:
        # Var(Δu) for this window
        var_delta_u = df["delta_u_real"].rolling(window_label, min_periods=2).var().fillna(0.0)
        
        # Cov(contrib_i, Δu) for each contributor
        for contrib_name in ["withdraw", "repay", "liquidate", "borrow", "supply"]:
            contrib_col = f"contrib_u_from_{contrib_name}"
            if contrib_col not in df.columns:
                continue
            
            # Compute rolling covariance
            cov = df[contrib_col].rolling(window_label, min_periods=2).cov(df["delta_u_real"]).fillna(0.0)
            
            # Share = Cov / Var (guard against near-zero variance)
            var_delta_u_safe = np.maximum(var_delta_u.to_numpy(), 1e-12)
            share = cov.to_numpy() / var_delta_u_safe
            
            df[f"vol_share_{contrib_name}_{window_label}"] = share
    
    # ==========================================
    # F) EXPANDING WINDOWS (ALL-TIME STATISTICS)
    # ==========================================
    
    df["util_mean_expanding"] = df["utilization_rate"].expanding(min_periods=1).mean()
    df["util_std_expanding"] = df["utilization_rate"].expanding(min_periods=2).std().fillna(0.0)
    df["util_max_expanding"] = df["utilization_rate"].expanding(min_periods=1).max()
    
    df["borrow_apr_mean_expanding"] = df["borrow_apr"].expanding(min_periods=1).mean()
    df["borrow_apr_std_expanding"] = df["borrow_apr"].expanding(min_periods=2).std().fillna(0.0)
    df["borrow_apr_max_expanding"] = df["borrow_apr"].expanding(min_periods=1).max()
    
    df["delta_u_std_expanding"] = df["delta_u_real"].expanding(min_periods=2).std().fillna(0.0)
    
    # ==========================================
    # RESET INDEX AND CONVERT TO ARROW
    # ==========================================
    
    df = df.reset_index(drop=True)
    
    # Convert back to Arrow table
    # Let Arrow infer schema from pandas, then we can optionally cast if needed
    attribution_table = pa.Table.from_pandas(df, preserve_index=False)
    
    return attribution_table


def validate_attribution_integrity(
    *,
    table: pa.Table,
    max_residual_abs: float = 0.01,
    check_sample_size: int = 100,
) -> dict[str, any]:
    """
    Validate attribution feature table integrity.
    
    Checks:
        - No NaNs in integer monetary columns
        - Utilization always in [0, 1]
        - Flow decomposition identity holds
        - Residuals are small in magnitude
        - AccrueInterest rows satisfy delta equality
    
    Args:
        table: Attribution feature table
        max_residual_abs: Maximum acceptable absolute residual
        check_sample_size: Sample size for detailed checks
    
    Returns:
        Dictionary with validation results
    """
    df = table.to_pandas()
    
    results = {
        "passed": True,
        "errors": [],
        "warnings": [],
    }
    
    # Check 1: No NaNs in integer monetary columns
    int_cols = [
        "delta_supply_assets", "delta_borrow_assets", "delta_collateral_assets",
        "total_supplied_assets", "outstanding_borrow_assets", "total_collateral_assets",
        "headroom_u90_assets", "headroom_u95_assets",
    ]
    for col in int_cols:
        if col in df.columns:
            if df[col].isna().any():
                results["passed"] = False
                results["errors"].append(f"Column {col} contains NaN values")
    
    # Check 2: Utilization in [0, 1]
    if "utilization_rate" in df.columns:
        util = df["utilization_rate"]
        if (util < 0).any() or (util > 1).any():
            results["passed"] = False
            results["errors"].append(f"Utilization out of bounds: min={util.min()}, max={util.max()}")
    
    # Check 3: Flow decomposition identity
    # delta_borrow_assets should equal: +borrow_in - repay_out - liquidate_repay + interest
    if all(col in df.columns for col in ["delta_borrow_assets", "borrow_in_assets", "repay_out_assets", 
                                           "liquidate_repay_assets", "interest_assets"]):
        reconstructed = (
            df["borrow_in_assets"] - df["repay_out_assets"] - 
            df["liquidate_repay_assets"] + df["interest_assets"]
        )
        delta_actual = df["delta_borrow_assets"].astype("float64")
        diff = (reconstructed - delta_actual).abs()
        
        if diff.max() > 1.0:  # Allow small numerical errors
            results["passed"] = False
            results["errors"].append(
                f"Flow decomposition identity violated: max_diff={diff.max()}, "
                f"mean_diff={diff.mean()}"
            )
    
    # Check 4: Residuals are small
    if "delta_u_residual" in df.columns:
        residuals = df["delta_u_residual"].abs()
        large_residuals = residuals > max_residual_abs
        
        if large_residuals.sum() > len(df) * 0.05:  # More than 5% have large residuals
            results["warnings"].append(
                f"{large_residuals.sum()} rows ({100*large_residuals.sum()/len(df):.1f}%) "
                f"have residuals > {max_residual_abs}. Max residual: {residuals.max()}"
            )
    
    # Check 5: AccrueInterest rows satisfy delta equality (feeShare=0)
    if "event_type" in df.columns:
        accrue_mask = df["event_type"] == "AccrueInterest"
        if accrue_mask.any():
            accrue_df = df[accrue_mask]
            
            if all(col in accrue_df.columns for col in ["delta_supply_assets", "delta_borrow_assets", "interest_assets"]):
                supply_delta = accrue_df["delta_supply_assets"].astype("float64")
                borrow_delta = accrue_df["delta_borrow_assets"].astype("float64")
                interest = accrue_df["interest_assets"]
                
                # For feeShare=0: delta_supply == delta_borrow == interest
                if not np.allclose(supply_delta, interest, atol=1.0):
                    results["warnings"].append(
                        "AccrueInterest rows: delta_supply_assets != interest_assets "
                        f"(max_diff={(supply_delta - interest).abs().max()})"
                    )
                if not np.allclose(borrow_delta, interest, atol=1.0):
                    results["warnings"].append(
                        "AccrueInterest rows: delta_borrow_assets != interest_assets "
                        f"(max_diff={(borrow_delta - interest).abs().max()})"
                    )
    
    return results
