"""
Validation utilities for data quality and schema enforcement.
"""
from __future__ import annotations

from typing import Sequence

import numpy as np
import pyarrow as pa


def validate_no_nulls_int(table: pa.Table, *, cols: Sequence[str], context: str) -> None:
    """
    Validate that integer columns have no null values.
    
    Args:
        table: Table to validate
        cols: Column names to check
        context: Context for error messages
    
    Raises:
        KeyError: If column is missing
        ValueError: If nulls are found
        TypeError: If column is not int64
    """
    for col in cols:
        if col not in table.column_names:
            raise KeyError(f"{context}: missing integer column {col}")
        arr = table[col]
        null_count = arr.null_count
        if null_count and null_count > 0:
            raise ValueError(f"{context}: {col} has {null_count} nulls")
        if not pa.types.is_int64(arr.type):
            raise TypeError(f"{context}: {col} must be int64; got {arr.type}")


def validate_utilization_bounds(table: pa.Table, *, col: str = "utilization_rate", context: str) -> None:
    """
    Validate that utilization rate is within [0, 1] bounds.
    
    Args:
        table: Table to validate
        col: Column name for utilization rate
        context: Context for error messages
    
    Raises:
        KeyError: If column is missing
        ValueError: If values are out of bounds or contain NaNs
    """
    if col not in table.column_names:
        raise KeyError(f"{context}: missing {col}")
    arr = table[col]
    values = arr.to_numpy(zero_copy_only=False).astype("float64")
    if np.isnan(values).any():
        raise ValueError(f"{context}: {col} contains NaNs")
    if (values < -1e-12).any() or (values > 1 + 1e-12).any():
        raise ValueError(f"{context}: {col} out of bounds [0,1]. min={values.min()}, max={values.max()}")


def validate_schema(table: pa.Table, *, expected: pa.Schema, context: str) -> None:
    """
    Validate that table matches expected schema.
    
    Args:
        table: Table to validate
        expected: Expected schema
        context: Context for error messages
    
    Raises:
        ValueError: If schema doesn't match
    """
    if not table.schema.equals(expected, check_metadata=False):
        actual_names = set(table.schema.names)
        expected_names = set(expected.names)
        missing = expected_names - actual_names
        extra = actual_names - expected_names
        if missing or extra:
            raise ValueError(
                f"{context}: Schema mismatch. Missing fields: {missing}. Extra fields: {extra}"
            )
