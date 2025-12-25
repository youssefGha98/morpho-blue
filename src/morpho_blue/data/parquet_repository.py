from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Sequence, Union

import duckdb
import pyarrow as pa

EventName = str


@dataclass(frozen=True)
class ParquetRepository:
    """
    Data access only. Returns pyarrow.Table.
    """
    con: duckdb.DuckDBPyConnection
    root: str  # directory containing morpho/market/shards/...

    def event_glob(self, event: EventName) -> str:
        return f"{self.root}/morpho/market/shards/{event}/shard_*.parquet"

    def read_event(
        self,
        *,
        event: EventName,
        market_id: str,
        id_col: str = "id",
        columns: Union[str, Iterable[str]] = "*",
        extra_where_sql: str = "",
        extra_params: Optional[Sequence[object]] = None,
    ) -> pa.Table:
        cols_sql = columns if isinstance(columns, str) else ", ".join(columns)
        parquet_glob = self.event_glob(event)

        sql = f"""
            SELECT {cols_sql}
            FROM read_parquet(?)
            WHERE {id_col} = ?
            {extra_where_sql}
        """
        params: list[object] = [parquet_glob, market_id]
        if extra_params:
            params.extend(list(extra_params))

        # DuckDB -> Arrow
        # duckdb python supports fetch_arrow_table()
        return self.con.execute(sql, params).fetch_arrow_table()
