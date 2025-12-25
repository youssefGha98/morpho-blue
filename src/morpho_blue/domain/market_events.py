from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pyarrow as pa

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


@dataclass(frozen=True)
class MarketEventTables:
    """
    Data-only wrapper of raw event tables for a market.
    All tables are pyarrow.Table (not pandas).
    """
    market_id: str

    supplies: pa.Table
    withdraws: pa.Table
    borrows: pa.Table
    repays: pa.Table
    liquidates: pa.Table
    accrue_interests: pa.Table
    collateral_supplies: pa.Table
    collateral_withdraws: pa.Table
