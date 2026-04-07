from __future__ import annotations

import sqlite3
from unittest import mock

import pytest

from forward.state_store import load_state as load_legacy_state
from forward.state_store_sqlite import (
    MIGRATIONS,
    SCHEMA_VERSION,
    OpenPositionState,
    RunnerState,
    SQLiteStateStore,
)


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


# ---------------------------------------------------------------------------
# Schema versioning tests
# ---------------------------------------------------------------------------


def test_fresh_db_sets_schema_version(tmp_path) -> None:
    """A brand-new database should be stamped with the latest schema version."""
    db_path = tmp_path / "state.db"
    with SQLiteStateStore(db_path=db_path) as store:
        conn = store._require_conn()
        row = conn.execute("SELECT version FROM schema_version WHERE id = 1").fetchone()
        assert row is not None
        assert int(row[0]) == SCHEMA_VERSION


def test_schema_version_table_has_updated_at(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    with SQLiteStateStore(db_path=db_path) as store:
        conn = store._require_conn()
        row = conn.execute("SELECT updated_at FROM schema_version WHERE id = 1").fetchone()
        assert row is not None
        assert row[0] is not None  # timestamp string


def test_pre_versioning_db_is_stamped_as_v1(tmp_path) -> None:
    """A database created before versioning was added should be detected and
    stamped as v1 without re-running migration 1."""
    db_path = tmp_path / "state.db"

    # Simulate a pre-versioning database: create tables manually without
    # schema_version.
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE runner_state (
          id INTEGER PRIMARY KEY,
          last_bar_open_time_utc TEXT,
          bars_processed INTEGER,
          current_day_utc TEXT,
          order_rejects_today INTEGER,
          daily_loss_halted INTEGER,
          drawdown_halted INTEGER,
          updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE open_positions (
          id INTEGER PRIMARY KEY,
          symbol TEXT, side TEXT, qty REAL, entry_price REAL,
          entry_time_utc TEXT, entry_order_id INTEGER,
          tp_order_id INTEGER, sl_order_id INTEGER,
          tp_price REAL, sl_price REAL, opened_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE trade_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          event_type TEXT, symbol TEXT, side TEXT, qty REAL, price REAL,
          realized_pnl REAL, fee REAL, funding_applied REAL,
          reason TEXT, bar_time_utc TEXT, recorded_at TEXT
        )
        """
    )
    # Insert some data to prove it survives the upgrade.
    conn.execute(
        "INSERT INTO runner_state (id, bars_processed, updated_at) VALUES (1, 42, '2024-01-01')"
    )
    conn.commit()
    conn.close()

    # Open with the versioned store — should detect and stamp v1.
    with SQLiteStateStore(db_path=db_path) as store:
        conn = store._require_conn()
        row = conn.execute("SELECT version FROM schema_version WHERE id = 1").fetchone()
        assert row is not None
        assert int(row[0]) >= 1

        # Pre-existing data must survive.
        state = store.load_state()
        assert state.bars_processed == 42


def test_reopen_does_not_rerun_migrations(tmp_path) -> None:
    """Opening an already-versioned database should not re-execute migrations."""
    db_path = tmp_path / "state.db"

    with SQLiteStateStore(db_path=db_path) as store:
        store.save_state(RunnerState(bars_processed=10))

    # Reopen — should be a no-op for migrations.
    with SQLiteStateStore(db_path=db_path) as store:
        state = store.load_state()
        assert state.bars_processed == 10

        conn = store._require_conn()
        row = conn.execute("SELECT version FROM schema_version WHERE id = 1").fetchone()
        assert int(row[0]) == SCHEMA_VERSION


def test_incremental_migration_runs_only_pending(tmp_path) -> None:
    """If a database is at v1, only migrations >1 should execute."""
    db_path = tmp_path / "state.db"

    # Create a v1 database.
    with SQLiteStateStore(db_path=db_path) as store:
        store.save_state(RunnerState(bars_processed=5))

    # Patch MIGRATIONS to add a v2 that adds a column to trade_log.
    v2_sql = "ALTER TABLE trade_log ADD COLUMN order_id INTEGER;"
    extended_migrations = list(MIGRATIONS) + [(2, v2_sql)]

    with mock.patch("forward.state_store_sqlite.MIGRATIONS", extended_migrations):
        with SQLiteStateStore(db_path=db_path) as store:
            conn = store._require_conn()

            # Version should now be 2.
            row = conn.execute("SELECT version FROM schema_version WHERE id = 1").fetchone()
            assert int(row[0]) == 2

            # The new column should exist.
            info = conn.execute("PRAGMA table_info(trade_log)").fetchall()
            col_names = [r[1] for r in info]
            assert "order_id" in col_names

            # Pre-existing state should survive.
            state = store.load_state()
            assert state.bars_processed == 5


def test_migration_failure_rolls_back(tmp_path) -> None:
    """A failing migration must not leave the version partially updated."""
    db_path = tmp_path / "state.db"

    # Create a v1 database.
    with SQLiteStateStore(db_path=db_path) as store:
        store.save_state(RunnerState(bars_processed=7))

    # Patch MIGRATIONS with a v2 that will fail (duplicate column).
    bad_sql = "ALTER TABLE trade_log ADD COLUMN event_type TEXT;"  # already exists
    extended_migrations = list(MIGRATIONS) + [(2, bad_sql)]

    with mock.patch("forward.state_store_sqlite.MIGRATIONS", extended_migrations):
        with pytest.raises(Exception):
            SQLiteStateStore(db_path=db_path).open()

    # Reopen with original migrations — version should still be 1 and data intact.
    with SQLiteStateStore(db_path=db_path) as store:
        conn = store._require_conn()
        row = conn.execute("SELECT version FROM schema_version WHERE id = 1").fetchone()
        assert int(row[0]) == 1
        state = store.load_state()
        assert state.bars_processed == 7


def test_migrations_are_sequential(tmp_path) -> None:
    """Migration version numbers must be sequential starting from 1."""
    versions = [v for v, _ in MIGRATIONS]
    assert versions == list(range(1, len(MIGRATIONS) + 1))


def test_schema_version_constant_matches_last_migration() -> None:
    assert SCHEMA_VERSION == MIGRATIONS[-1][0]
