"""Regression tests for the entry-price recovery + bracket-price backstop.

A live market entry sometimes returns without a usable ``avgPrice`` (an
ACK-style payload), leaving ``entry_price`` at 0.0. For an ``entry_pct`` target
that silently collapses the take-profit to 0.0, which Binance rejects, tripping
the protection-missing emergency flatten. These tests cover the two defenses:

  1. Re-fetch the real fill price from the exchange position so the bracket is
     computed correctly and the position stays protected.
  2. If recovery also fails, flatten immediately with ``invalid_bracket_price``
     instead of submitting a doomed 0-price protective order.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from forward.state_store_sqlite import SQLiteStateStore
from tests.helpers import LONG_PCT_SIGNAL
from tests.integration.mocks import FakeBinanceClient, build_trader_service


class _CountingBroker(FakeBinanceClient):
    """Broker whose entry response omits a usable avgPrice and that counts
    protective-order placements."""

    def __init__(self, *, exchange_reports_zero_entry: bool, fill_price: float = 100.0) -> None:
        super().__init__(fill_price=float(fill_price))
        self._exchange_reports_zero_entry = bool(exchange_reports_zero_entry)
        self.tp_placed = 0
        self.sl_placed = 0

    def place_market_order(
        self,
        *,
        symbol: str,
        side: str,
        quantity: float,
        reduce_only: bool = False,
        reference_price: float | None = None,
        client_order_id: str | None = None,
    ) -> Any:
        resp = super().place_market_order(
            symbol=symbol,
            side=side,
            quantity=quantity,
            reduce_only=reduce_only,
            reference_price=reference_price,
            client_order_id=client_order_id,
        )
        if not bool(reduce_only):
            resp = dict(resp)
            resp["avgPrice"] = "0"  # ACK-style: no usable fill price in the response
            if self._exchange_reports_zero_entry:
                # The exchange position also can't supply a price -> recovery fails.
                self._entry_price = 0.0
        return resp

    def place_take_profit_market(self, **kwargs: Any) -> Any:
        self.tp_placed += 1
        return super().place_take_profit_market(**kwargs)

    def place_stop_market(self, **kwargs: Any) -> Any:
        self.sl_placed += 1
        return super().place_stop_market(**kwargs)


def _long_pct_row() -> pd.Series:
    # LONG_PCT_SIGNAL: entry_pct target (0.02), stop_kind=orb_low.
    return pd.Series({"signal": 1, "signal_type": LONG_PCT_SIGNAL, "close": 100.0, "orb_low": 95.0})


def _run_entry(broker: _CountingBroker, tmp_path: Path) -> list[dict[str, Any]]:
    db_path = tmp_path / "state.db"
    with SQLiteStateStore(db_path=db_path) as store:
        state = store.load_state()
        trader = build_trader_service(
            broker=broker,
            store=store,
            state=state,
            work_dir=tmp_path,
            leverage=1.0,
            position_size=0.1,
            initial_capital=1000.0,
            max_order_rejects_per_day=10,
        )

        emitted: list[dict[str, Any]] = []
        trader.emit_event = lambda rows: emitted.extend(rows)
        trader.append_rows = lambda *args, **kwargs: None

        asyncio.run(trader.maybe_place_trade_from_signal(pd.Timestamp("2026-01-01T00:00:00Z"), _long_pct_row()))
        return emitted


def _types(events: list[dict[str, Any]]) -> set[str]:
    return {str(e.get("type") or "") for e in events}


def test_entry_price_recovered_keeps_position_protected(tmp_path: Path) -> None:
    broker = _CountingBroker(exchange_reports_zero_entry=False, fill_price=100.0)

    db_path = tmp_path / "state.db"
    with SQLiteStateStore(db_path=db_path) as store:
        state = store.load_state()
        trader = build_trader_service(
            broker=broker,
            store=store,
            state=state,
            work_dir=tmp_path,
            leverage=1.0,
            position_size=0.1,
            initial_capital=1000.0,
            max_order_rejects_per_day=10,
        )
        emitted: list[dict[str, Any]] = []
        trader.emit_event = lambda rows: emitted.extend(rows)
        trader.append_rows = lambda *args, **kwargs: None

        asyncio.run(trader.maybe_place_trade_from_signal(pd.Timestamp("2026-01-01T00:00:00Z"), _long_pct_row()))

        # Recovery fired and produced the real fill price.
        recovered = [e for e in emitted if str(e.get("type") or "") == "ENTRY_PRICE_RECOVERED"]
        assert len(recovered) == 1
        assert float(recovered[0]["recovered_entry_price"]) == pytest.approx(100.0, abs=1e-9)

        # Position is held and protected; no bail-out happened.
        assert trader.state.open_position is not None
        assert float(trader.state.open_position.entry_price) == pytest.approx(100.0, abs=1e-9)
        assert broker.tp_placed == 1
        assert broker.sl_placed == 1
        assert "BRACKET_SKIPPED" not in _types(emitted)
        assert "EMERGENCY_FLATTEN_SUBMIT" not in _types(emitted)


def test_invalid_bracket_price_flattens_without_placing_zero_price_tp(tmp_path: Path) -> None:
    broker = _CountingBroker(exchange_reports_zero_entry=True, fill_price=100.0)

    emitted = _run_entry(broker, tmp_path)

    # Recovery was attempted and failed.
    assert "ENTRY_PRICE_RECOVERY_FAILED" in _types(emitted)

    # The backstop fired: skipped the bracket and flattened, never sending a TP.
    skipped = [e for e in emitted if str(e.get("type") or "") == "BRACKET_SKIPPED"]
    assert len(skipped) == 1
    assert skipped[0]["reason"] == "invalid_bracket_price"
    assert float(skipped[0]["details"]["tp_price"]) == pytest.approx(0.0, abs=1e-9)

    assert "EMERGENCY_FLATTEN_SUBMIT" in _types(emitted)
    assert broker.tp_placed == 0
    assert broker.sl_placed == 0
