from __future__ import annotations

import pytest

from forward.state_store import load_state as load_legacy_state
from forward.state_store_sqlite import OpenPositionState, RunnerState, SQLiteStateStore


def test_sqlite_state_store_round_trip_with_crash_reopen(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    expected = RunnerState(
        last_bar_open_time_utc="2024-01-15T14:30:00+00:00",
        bars_processed=321,
        current_day_utc="2024-01-15",
        order_rejects_today=3,
        daily_loss_halted=True,
        drawdown_halted=True,
        open_position=OpenPositionState(
            symbol="BTCUSDT",
            side="LONG",
            qty=1.25,
            entry_price=102.5,
            entry_time_utc="2024-01-15T14:00:00+00:00",
            entry_order_id=123456,
            tp_order_id=123457,
            sl_order_id=123458,
            tp_price=110.0,
            sl_price=95.0,
            opened_at="2024-01-15T14:00:05+00:00",
        ),
    )

    store = SQLiteStateStore(db_path=db_path)
    store.open()
    store.save_state(expected)

    # Crash simulation: close raw sqlite connection directly without store.close().
    assert store.conn is not None
    store.conn.close()

    reopened = SQLiteStateStore(db_path=db_path)
    reopened.open()
    loaded = reopened.load_state()
    reopened.close()

    assert loaded.last_bar_open_time_utc == expected.last_bar_open_time_utc
    assert loaded.bars_processed == expected.bars_processed
    assert loaded.current_day_utc == expected.current_day_utc
    assert loaded.order_rejects_today == expected.order_rejects_today
    assert loaded.daily_loss_halted == expected.daily_loss_halted
    assert loaded.drawdown_halted == expected.drawdown_halted

    assert loaded.open_position is not None
    assert expected.open_position is not None
    assert loaded.open_position.symbol == expected.open_position.symbol
    assert loaded.open_position.side == expected.open_position.side
    assert loaded.open_position.qty == pytest.approx(expected.open_position.qty, abs=1e-12)
    assert loaded.open_position.entry_price == pytest.approx(
        expected.open_position.entry_price, abs=1e-12
    )
    assert loaded.open_position.entry_time_utc == expected.open_position.entry_time_utc
    assert loaded.open_position.entry_order_id == expected.open_position.entry_order_id
    assert loaded.open_position.tp_order_id == expected.open_position.tp_order_id
    assert loaded.open_position.sl_order_id == expected.open_position.sl_order_id
    assert loaded.open_position.tp_price == pytest.approx(expected.open_position.tp_price, abs=1e-12)
    assert loaded.open_position.sl_price == pytest.approx(expected.open_position.sl_price, abs=1e-12)
    assert loaded.open_position.opened_at == expected.open_position.opened_at


def test_json_snapshot_round_trip_preserves_opened_at(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    json_path = tmp_path / "state.json"
    state = RunnerState(
        open_position=OpenPositionState(
            symbol="BTCUSDT",
            side="LONG",
            qty=1.0,
            entry_price=100.0,
            entry_time_utc="2024-01-15T14:00:00+00:00",
            entry_order_id=123456,
            tp_order_id=123457,
            sl_order_id=123458,
            tp_price=110.0,
            sl_price=95.0,
            opened_at="2024-01-15T14:00:05+00:00",
        ),
    )

    with SQLiteStateStore(db_path=db_path) as store:
        store.export_state_json_snapshot(json_path, state)

    loaded = load_legacy_state(json_path)
    assert loaded.open_position is not None
    assert loaded.open_position.opened_at == "2024-01-15T14:00:05+00:00"


def test_save_state_assigns_opened_at_when_missing(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    state = RunnerState(
        open_position=OpenPositionState(
            symbol="BTCUSDT",
            side="LONG",
            qty=1.0,
            entry_price=100.0,
            entry_time_utc="2024-01-15T14:00:00+00:00",
            entry_order_id=123456,
        ),
    )

    with SQLiteStateStore(db_path=db_path) as store:
        store.save_state(state)
        loaded = store.load_state()

    assert state.open_position is not None
    assert state.open_position.opened_at is not None
    assert loaded.open_position is not None
    assert loaded.open_position.opened_at == state.open_position.opened_at
