"""Watermark extraction and persistence helpers."""

from __future__ import annotations

from typing import Any, Optional


class WatermarkError(Exception):
    """Raised when a watermark cannot be computed."""


def max_watermark(df: Any, watermark_column: str) -> Optional[str]:
    """Return the maximum watermark value in a DataFrame as a string."""

    if watermark_column not in df.columns:
        raise WatermarkError(f"Watermark column not found: {watermark_column}")

    row = df.selectExpr(f"max({watermark_column}) as watermark").first()
    value = row["watermark"] if row else None
    return str(value) if value is not None else None

