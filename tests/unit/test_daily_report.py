from __future__ import annotations

import sqlite3

from ops.daily_report import (
    _format_message,
    _load_all_time_net,
    _load_latest_trade,
    _load_trades,
)

# trade_log schema mirrors forward/state_store_sqlite.py migration v1.
_CREATE_TRADE_LOG = """
CREATE TABLE trade_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_type TEXT,
  symbol TEXT,
  side TEXT,
  qty REAL,
  price REAL,
  realized_pnl REAL,
  fee REAL,
  funding_applied REAL,
  reason TEXT,
  bar_time_utc TEXT,
  recorded_at TEXT
);
"""

_COLUMNS = (
    "event_type, symbol, side, qty, price, realized_pnl, fee, "
    "funding_applied, reason, bar_time_utc, recorded_at"
)


def _new_db(tmp_path, rows):
    """Create a trade_log DB and insert ``rows`` in order (ids auto-assigned)."""
    db_path = tmp_path / "state.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(_CREATE_TRADE_LOG)
        for r in rows:
            conn.execute(
                f"INSERT INTO trade_log ({_COLUMNS}) "
                f"VALUES (:event_type, :symbol, :side, :qty, :price, :realized_pnl, "
                f":fee, :funding_applied, :reason, :bar_time_utc, :recorded_at)",
                r,
            )
        conn.commit()
    finally:
        conn.close()
    return db_path


def _entry(symbol="BTCUSDT", side="LONG", price=100.0, qty=1.0, t="2026-06-20T10:00:00+00:00"):
    return {
        "event_type": "ENTRY",
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "price": price,
        "realized_pnl": None,
        "fee": 0.0,
        "funding_applied": None,
        "reason": None,
        "bar_time_utc": t,
        "recorded_at": t,
    }


def _exit(
    symbol="BTCUSDT",
    side="LONG",
    price=110.0,
    qty=1.0,
    realized_pnl=10.0,
    reason="tp",
    funding_applied=0.0,
    t="2026-06-20T11:00:00+00:00",
):
    return {
        "event_type": "EXIT",
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "price": price,
        "realized_pnl": realized_pnl,
        "fee": 0.5,
        "funding_applied": funding_applied,
        "reason": reason,
        "bar_time_utc": t,
        "recorded_at": t,
    }


def _event(event_type, symbol="BTCUSDT", reason="KILL_SWITCH_MARGIN_RATIO"):
    return {
        "event_type": event_type,
        "symbol": symbol,
        "side": None,
        "qty": None,
        "price": None,
        "realized_pnl": None,
        "fee": None,
        "funding_applied": None,
        "reason": reason,
        "bar_time_utc": "2026-06-20T10:30:00+00:00",
        "recorded_at": "2026-06-20T10:30:00+00:00",
    }


def test_entry_matches_real_exit_and_surfaces_reason(tmp_path):
    db = _new_db(tmp_path, [_entry(), _exit(realized_pnl=10.0, reason="tp")])
    trades = _load_trades(db, n=30)
    assert len(trades) == 1
    assert trades[0]["realized_pnl"] == 10.0
    assert trades[0]["exit_reason"] == "tp"
    assert trades[0]["exit_price"] == 110.0


def test_phantom_null_pnl_exit_is_skipped_not_dropped(tmp_path):
    # ENTRY, phantom EXIT (NULL pnl), then the real EXIT for the SAME entry.
    rows = [
        _entry(t="2026-06-20T10:00:00+00:00"),
        _exit(
            realized_pnl=None,
            reason="EMERGENCY_FLATTEN:SHUTDOWN_GUARD",
            price=None,
            t="2026-06-20T10:15:00+00:00",
        ),
        _exit(realized_pnl=25.0, reason="sl", price=108.0, t="2026-06-20T11:00:00+00:00"),
    ]
    db = _new_db(tmp_path, rows)
    trades = _load_trades(db, n=30)
    # The completed trade must NOT be dropped; it pairs to the real exit.
    assert len(trades) == 1
    assert trades[0]["realized_pnl"] == 25.0
    assert trades[0]["exit_reason"] == "sl"


def test_non_trade_event_rows_are_ignored_by_matcher(tmp_path):
    rows = [
        _event("REJECT"),
        _event("ENTRY_FAILED"),
        _entry(t="2026-06-20T10:00:00+00:00"),
        _event("KILL_SWITCH"),  # between entry and exit, must be ignored
        _exit(realized_pnl=12.0, reason="tp", t="2026-06-20T11:00:00+00:00"),
    ]
    db = _new_db(tmp_path, rows)
    trades = _load_trades(db, n=30)
    assert len(trades) == 1
    assert trades[0]["realized_pnl"] == 12.0


def test_all_time_net_counts_every_trade_and_excludes_null_pnl(tmp_path):
    rows = [
        _entry(t="2026-06-20T10:00:00+00:00"),
        _exit(realized_pnl=10.0, funding_applied=1.0, t="2026-06-20T11:00:00+00:00"),
        _entry(t="2026-06-20T12:00:00+00:00"),
        _exit(realized_pnl=None, reason="EMERGENCY_FLATTEN:x", price=None, t="2026-06-20T12:30:00+00:00"),
        _entry(t="2026-06-20T13:00:00+00:00"),
        _exit(realized_pnl=-4.0, funding_applied=0.0, reason="sl", t="2026-06-20T14:00:00+00:00"),
        _event("KILL_SWITCH"),
    ]
    db = _new_db(tmp_path, rows)
    all_time = _load_all_time_net(db)
    # Two completed (non-null) exits; the phantom and the event row are excluded.
    assert all_time["count"] == 2
    # net = (10 - 1) + (-4 - 0) = 5.0
    assert all_time["net"] == 5.0


def test_latest_trade_is_most_recent_non_null_exit(tmp_path):
    rows = [
        _entry(t="2026-06-20T10:00:00+00:00"),
        _exit(realized_pnl=10.0, reason="tp", t="2026-06-20T11:00:00+00:00"),
        _entry(t="2026-06-20T12:00:00+00:00"),
        _exit(realized_pnl=-3.0, funding_applied=0.5, reason="sl", t="2026-06-20T13:00:00+00:00"),
        # phantom exit after the real one: must NOT be reported as latest
        _exit(realized_pnl=None, reason="EMERGENCY_FLATTEN:x", price=None, t="2026-06-20T13:30:00+00:00"),
    ]
    db = _new_db(tmp_path, rows)
    latest = _load_latest_trade(db)
    assert latest is not None
    assert latest["reason"] == "sl"
    assert latest["net"] == -3.5  # -3.0 - 0.5 funding


def test_rendered_report_contains_new_lines_with_correct_values(tmp_path):
    rows = [
        _entry(t="2026-06-20T10:00:00+00:00"),
        _exit(realized_pnl=10.0, reason="tp", t="2026-06-20T11:00:00+00:00"),
        _entry(t="2026-06-20T12:00:00+00:00"),
        _exit(realized_pnl=-3.0, reason="sl", t="2026-06-20T13:00:00+00:00"),
    ]
    db = _new_db(tmp_path, rows)

    from ops.daily_report import _compute_metrics

    trades = list(reversed(_load_trades(db, n=30)))
    metrics = _compute_metrics(trades)
    metrics["latest_trade"] = _load_latest_trade(db)
    metrics["all_time"] = _load_all_time_net(db)

    msg = _format_message(metrics, "BTCUSDT", "2026-06-21 00:00 UTC")

    assert "Latest trade:    -3.00 USD (sl)" in msg
    assert "Window net:      +7.00 USD (last 2 trades)" in msg
    assert "All-time net:    +7.00 USD  (2 trades)" in msg
    assert "Net return:" not in msg  # relabeled away
