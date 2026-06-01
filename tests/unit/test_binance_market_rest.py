from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, List

import pandas as pd

from forward.binance_live import (
    BinanceLiveKlineSource,
    BinanceRestKlineSource,
    fetch_recent_klines_df,
)
from forward.data_service import DataService


class _FakeResp:
    def __init__(self, payload: Any) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        return self._payload


class _FakeGetSession:
    """Stub of requests.Session that returns a canned klines payload from .get()."""

    def __init__(self, payload: Any) -> None:
        self._payload = payload
        self.calls: List[tuple] = []

    def get(self, url: str, params: Any = None, timeout: Any = None) -> _FakeResp:
        self.calls.append((url, dict(params or {}), timeout))
        return _FakeResp(self._payload)


def _kline(open_ms: int, close_ms: int, *, o: str, h: str, low: str, c: str, v: str) -> list:
    # Binance kline schema: [openTime, open, high, low, close, volume, closeTime, ...]
    return [open_ms, o, h, low, c, v, close_ms, "0", 0, "0", "0", "0"]


def test_fetch_recent_klines_df_keeps_only_closed_candles() -> None:
    bar_ms = 60_000
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    closed_open = now_ms - 3 * bar_ms          # closeTime well in the past -> kept
    open_open = now_ms                          # closeTime in the future -> dropped
    payload = [
        _kline(closed_open, closed_open + bar_ms - 1, o="10", h="12", low="9", c="11", v="100"),
        _kline(open_open, open_open + bar_ms - 1, o="11", h="13", low="10", c="12", v="50"),
    ]
    sess = _FakeGetSession(payload)

    df, meta = fetch_recent_klines_df("BTCUSDT", "1m", limit=2, market="futures", session=sess)

    assert len(df) == 1
    assert meta["rows"] == 1
    assert list(df.columns) == ["open", "high", "low", "close", "volume"]
    kept = pd.to_datetime(closed_open, unit="ms", utc=True)
    dropped = pd.to_datetime(open_open, unit="ms", utc=True)
    assert kept in df.index
    assert dropped not in df.index
    assert float(df.loc[kept, "close"]) == 11.0
    # Futures klines path was used (spot would be /api/v3/klines).
    assert sess.calls and sess.calls[0][0].endswith("/fapi/v1/klines")


def _df(open_times_ms: List[int]) -> pd.DataFrame:
    idx = pd.DatetimeIndex(
        [pd.to_datetime(t, unit="ms", utc=True) for t in open_times_ms], name="open_time"
    )
    n = len(open_times_ms)
    data = {
        "open": [float(i + 1) for i in range(n)],
        "high": [float(i + 1) + 0.5 for i in range(n)],
        "low": [float(i + 1) - 0.5 for i in range(n)],
        "close": [float(i + 1) + 0.25 for i in range(n)],
        "volume": [float((i + 1) * 10) for i in range(n)],
    }
    return pd.DataFrame(data, index=idx)


def test_rest_source_primes_baseline_then_emits_new_bars() -> None:
    src = BinanceRestKlineSource(symbol="btcusdt", interval="1m")
    bar_ms = 60_000
    t0 = 1_700_000_040_000  # minute-aligned

    # First poll establishes a baseline at the most recent closed bar; emits nothing.
    df1 = _df([t0 - bar_ms, t0])
    assert src._new_bars(df1) == []
    assert src._last_emitted_open == pd.to_datetime(t0, unit="ms", utc=True)

    # A new closed bar appears -> exactly one new bar, with reconstructed close_time.
    df2 = _df([t0 - bar_ms, t0, t0 + bar_ms])
    bars = src._new_bars(df2)
    assert len(bars) == 1
    bar = bars[0]
    assert bar.symbol == "BTCUSDT"
    assert bar.open_time == pd.to_datetime(t0 + bar_ms, unit="ms", utc=True)
    expected_close = bar.open_time + pd.Timedelta(seconds=60) - pd.Timedelta(milliseconds=1)
    assert bar.close_time == expected_close

    # The stream advances the cursor as it yields; re-polling the same frame de-dupes.
    src._last_emitted_open = bar.open_time
    assert src._new_bars(df2) == []


def test_rest_source_empty_frame_is_tolerated() -> None:
    src = BinanceRestKlineSource(symbol="BTCUSDT", interval="1m")
    empty = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    empty.index = pd.DatetimeIndex([], tz="UTC")
    assert src._new_bars(empty) == []
    assert src._last_emitted_open is None


def test_sources_expose_kind() -> None:
    assert BinanceLiveKlineSource(symbol="BTCUSDT", interval="1m").kind == "ws"
    assert BinanceRestKlineSource(symbol="BTCUSDT", interval="1m").kind == "rest"


def test_data_service_selects_source_by_data_source_flag() -> None:
    common = dict(
        symbol="BTCUSDT",
        interval="1m",
        market="futures",
        stale_allowed_seconds=10.0,
        max_backoff_seconds=5,
        stale_check_interval_seconds=5,
        heartbeat_seconds=10,
        emit_event=lambda rows: None,
    )
    rest_ds = DataService(**common, data_source="rest")
    assert rest_ds.kind == "rest"
    assert rest_ds._src.kind == "rest"

    ws_ds = DataService(**common)  # default
    assert ws_ds.kind == "ws"
    assert ws_ds._src.kind == "ws"
