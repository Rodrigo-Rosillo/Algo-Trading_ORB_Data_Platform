from __future__ import annotations

import sys
import types
import uuid

import pandas as pd
import pytest

from backtester.futures_engine import FuturesEngineConfig
from core.strategy_plugin import StrategyBuildResult
from forward.binance_live import interval_to_seconds
from forward.shadow import build_signals, run_shadow_futures
from forward.stream_engine import StreamingFuturesShadowEngine
from tests.helpers import LONG_PCT_SIGNAL, build_test_execution_specs


def _market_df() -> pd.DataFrame:
    idx = pd.date_range("2024-01-01 00:00:00", periods=5, freq="30min", tz="UTC")
    return pd.DataFrame(
        {
            "open": [100.0, 100.0, 100.5, 101.0, 101.0],
            "high": [100.5, 101.0, 102.5, 101.5, 101.5],
            "low": [99.5, 99.5, 99.5, 100.5, 100.5],
            "close": [100.0, 100.0, 102.0, 101.0, 101.0],
            "volume": [1.0, 1.0, 1.0, 1.0, 1.0],
        },
        index=idx,
    )


def _register_plugin(df_raw: pd.DataFrame) -> str:
    module_name = f"tests.synthetic_replay_plugin_{uuid.uuid4().hex}"
    module = types.ModuleType(module_name)

    def build_strategy(**kwargs):
        _ = kwargs
        df_sig = df_raw.copy()
        df_sig["signal"] = [1, 0, 0, 0, 0]
        df_sig["signal_type"] = [LONG_PCT_SIGNAL, "", "", "", ""]
        df_sig["orb_low"] = [99.0, 99.0, 99.0, 99.0, 99.0]
        return StrategyBuildResult(
            df_sig=df_sig,
            execution_specs=build_test_execution_specs(),
            strategy_metadata={"case": "synthetic_replay"},
        )

    module.build_strategy = build_strategy
    sys.modules[module_name] = module
    return module_name


@pytest.fixture(scope="module")
def replay_results():
    df_raw = _market_df()
    module_name = _register_plugin(df_raw)
    cfg = {
        "symbol": "BTCUSDT",
        "timeframe": "30m",
        "risk": {
            "initial_capital": 1000.0,
            "position_size": 0.1,
        },
        "fees": {
            "taker_fee_rate": 0.0,
        },
        "leverage": {
            "enabled": True,
            "max_leverage": 1,
        },
        "funding": {
            "rate_per_8h": 0.0,
        },
        "strategy_plugin": {
            "module": module_name,
            "callable": "build_strategy",
        },
        "strategy": {},
    }
    valid_days = set(df_raw.index.date)

    batch_result = run_shadow_futures(
        df_raw=df_raw,
        valid_days=valid_days,
        cfg=cfg,
        delay_bars=1,
        slippage_bps=0.0,
        fee_mult=1.0,
        funding_rate_per_8h=0.0,
        risk_limits=None,
    )

    strategy_result = build_signals(
        df_raw=df_raw,
        valid_days=valid_days,
        cfg=cfg,
    )

    engine = StreamingFuturesShadowEngine(
        cfg=FuturesEngineConfig(
            initial_capital=1000.0,
            position_size=0.1,
            leverage=1.0,
            taker_fee_rate=0.0,
            fee_mult=1.0,
            slippage_bps=0.0,
            delay_bars=1,
            funding_rate_per_8h=0.0,
        ),
        risk_limits=None,
        expected_bar_seconds=interval_to_seconds("30m"),
        execution_specs=strategy_result.execution_specs,
    )

    last_close = None
    for ts, row in strategy_result.df_sig.iterrows():
        engine.on_bar(
            ts=ts,
            bar_open=float(row["open"]),
            bar_high=float(row["high"]),
            bar_low=float(row["low"]),
            bar_close=float(row["close"]),
            current_date=row.get("date"),
            signal=int(row.get("signal", 0) or 0),
            signal_type=str(row.get("signal_type", "") or ""),
            orb_high=None if pd.isna(row.get("orb_high")) else float(row.get("orb_high")),
            orb_low=None if pd.isna(row.get("orb_low")) else float(row.get("orb_low")),
            valid_days=valid_days,
        )
        last_close = float(row["close"])

    return {
        "batch": batch_result.trades,
        "streaming": list(engine.core.trades),
        "batch_final_equity": float(batch_result.equity_curve.iloc[-1]),
        "streaming_final_equity": float(engine.equity(last_close if last_close is not None else 1000.0)),
    }


def test_replay_trade_count(replay_results):
    assert len(replay_results["batch"]) == len(replay_results["streaming"]) == 1


def test_replay_entry_times(replay_results):
    assert replay_results["batch"][0]["entry_time"] == replay_results["streaming"][0]["entry_time"]


def test_replay_exit_times(replay_results):
    assert replay_results["batch"][0]["exit_time"] == replay_results["streaming"][0]["exit_time"]


def test_replay_pnl_per_trade(replay_results):
    assert float(replay_results["batch"][0]["pnl_net"]) == pytest.approx(
        float(replay_results["streaming"][0]["pnl_net"]),
        abs=1e-8,
    )


def test_replay_final_equity(replay_results):
    assert float(replay_results["batch_final_equity"]) == pytest.approx(
        float(replay_results["streaming_final_equity"]),
        abs=1e-8,
    )
