#!/usr/bin/env python
"""Validate the REST kline source against the WebSocket source.

Runs ``BinanceLiveKlineSource`` (ws) and ``BinanceRestKlineSource`` (rest) side by side
for a few minutes on the same symbol/interval and checks that they emit identical CLOSED
bars (same open_time keys, exact OHLCV). It also reports how long after the WS source the
REST poller detected each bar.

This is a manual/live tool — it talks to Binance and is NOT part of the unit suite.

Example:
    python scripts/validate_rest_kline_source.py --symbol BTCUSDT --interval 1m --minutes 3
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Tuple

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from forward.binance_live import BinanceLiveKlineSource, BinanceRestKlineSource


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def _collect(src: Any, stop_event: asyncio.Event, sink: Dict[str, Tuple[dict, datetime]]) -> None:
    async for bar in src.stream_closed(stop_event=stop_event):
        key = bar.open_time.isoformat()
        if key not in sink:
            sink[key] = (bar.to_row(), _utcnow())


async def _run(symbol: str, interval: str, market: str, minutes: float, limit: int) -> int:
    ws = BinanceLiveKlineSource(symbol=symbol, interval=interval, market=market)
    rest = BinanceRestKlineSource(symbol=symbol, interval=interval, market=market, klines_limit=limit)

    ws_bars: Dict[str, Tuple[dict, datetime]] = {}
    rest_bars: Dict[str, Tuple[dict, datetime]] = {}
    stop_event = asyncio.Event()

    print(f"Comparing ws vs rest for {symbol} {interval} ({market}) over {minutes:.1f} min ...")
    print(f"  ws.kind={ws.kind!r}  rest.kind={rest.kind!r}")

    tasks = [
        asyncio.create_task(_collect(ws, stop_event, ws_bars)),
        asyncio.create_task(_collect(rest, stop_event, rest_bars)),
    ]
    try:
        await asyncio.sleep(max(1.0, minutes * 60.0))
    finally:
        stop_event.set()
        await asyncio.gather(*tasks, return_exceptions=True)

    common = sorted(set(ws_bars) & set(rest_bars))
    only_ws = sorted(set(ws_bars) - set(rest_bars))
    only_rest = sorted(set(rest_bars) - set(ws_bars))

    print(f"\nws closed bars:   {len(ws_bars)}")
    print(f"rest closed bars: {len(rest_bars)}")
    print(f"common bars:      {len(common)}")

    mismatches = 0
    delays = []
    for key in common:
        ws_row, ws_at = ws_bars[key]
        rest_row, rest_at = rest_bars[key]
        delays.append((rest_at - ws_at).total_seconds())
        if ws_row != rest_row:
            mismatches += 1
            print(f"  MISMATCH {key}:\n    ws  ={ws_row}\n    rest={rest_row}")

    if delays:
        mean = sum(delays) / len(delays)
        print(f"\nREST-vs-WS detection delay (s): min={min(delays):.2f} mean={mean:.2f} max={max(delays):.2f}")

    # Edge-of-window bars (one source emits a boundary bar the other has not reached yet,
    # especially the REST source's settle delay near shutdown) are expected and reported
    # for information only; they do not fail the run.
    if only_ws:
        print(f"\nbars only on ws ({len(only_ws)}): {only_ws}")
    if only_rest:
        print(f"bars only on rest ({len(only_rest)}): {only_rest}")

    ok = bool(common) and mismatches == 0
    print(f"\nRESULT: {'PASS' if ok else 'FAIL'} (common={len(common)}, mismatches={mismatches})")
    return 0 if ok else 1


def main() -> int:
    p = argparse.ArgumentParser(description="Validate REST kline source parity vs the WebSocket source.")
    p.add_argument("--symbol", default="BTCUSDT")
    p.add_argument("--interval", default="1m", help="Kline interval (e.g. 1m, 30m, 1h).")
    p.add_argument("--market", default="futures", choices=["futures", "spot"])
    p.add_argument("--minutes", type=float, default=3.0, help="How long to compare, in minutes.")
    p.add_argument("--limit", type=int, default=5, help="REST klines limit per poll.")
    args = p.parse_args()
    return asyncio.run(_run(args.symbol, args.interval, args.market, args.minutes, args.limit))


if __name__ == "__main__":
    raise SystemExit(main())
