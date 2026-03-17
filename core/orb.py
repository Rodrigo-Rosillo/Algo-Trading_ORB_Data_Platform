from __future__ import annotations

from datetime import time
from typing import Tuple

import numpy as np
import pandas as pd


def calculate_adx(
    df: pd.DataFrame,
    period: int = 14,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Calculate ADX and directional indicators aligned to df.index."""
    high = df[high_col]
    low = df[low_col]
    close = df[close_col]

    high_diff = high.diff()
    low_diff = -low.diff()

    plus_dm = high_diff.copy()
    plus_dm[(high_diff < 0) | (high_diff < low_diff)] = 0.0

    minus_dm = low_diff.copy()
    minus_dm[(low_diff < 0) | (low_diff < high_diff)] = 0.0

    high_low = high - low
    high_close = (high - close.shift()).abs()
    low_close = (low - close.shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)

    atr = tr.ewm(alpha=1 / period, adjust=False).mean()
    atr_safe = atr.replace(0, np.nan)

    plus_di = 100 * (plus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr_safe)
    minus_di = 100 * (minus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr_safe)

    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
    adx = dx.ewm(alpha=1 / period, adjust=False).mean()

    return adx, plus_di, minus_di


def identify_orb_ranges(
    df: pd.DataFrame,
    orb_start_time: time,
    orb_end_time: time,
    high_col: str = "high",
    low_col: str = "low",
) -> pd.DataFrame:
    """Identify ORB high/low per day for a DatetimeIndex frame."""
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("identify_orb_ranges requires df.index to be a pandas DatetimeIndex")

    dates = df.index.date
    times = df.index.time
    in_orb = (times >= orb_start_time) & (times <= orb_end_time)
    orb = df.loc[in_orb, [high_col, low_col]].copy()
    orb = orb.assign(date=dates[in_orb])

    return (
        orb.groupby("date")
        .agg({high_col: "max", low_col: "min"})
        .rename(columns={high_col: "orb_high", low_col: "orb_low"})
    )


def add_trend_indicators(
    df: pd.DataFrame,
    period: int = 14,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
) -> pd.DataFrame:
    """Add ADX, directional indicators, and a simple trend label."""
    out = df.copy()
    adx, plus_di, minus_di = calculate_adx(
        out,
        period=period,
        high_col=high_col,
        low_col=low_col,
        close_col=close_col,
    )
    out["adx"] = adx
    out["plus_di"] = plus_di
    out["minus_di"] = minus_di
    out["trend"] = "sideways"
    out.loc[out["plus_di"] > out["minus_di"], "trend"] = "uptrend"
    out.loc[out["minus_di"] > out["plus_di"], "trend"] = "downtrend"
    return out
