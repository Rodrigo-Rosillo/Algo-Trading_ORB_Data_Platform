from __future__ import annotations

import sys
import types
import uuid

import pandas as pd
import pytest

from core.strategy_plugin import StrategyBuildResult, build_strategy_result
from tests.helpers import LONG_ENTRY_SIGNAL, SHORT_ENTRY_SIGNAL, SHORT_ORB_SIGNAL, build_test_execution_specs


def _market_df() -> pd.DataFrame:
    idx = pd.date_range("2024-01-01 00:00:00", periods=3, freq="30min", tz="UTC")
    return pd.DataFrame(
        {
            "open": [100.0, 100.0, 100.0],
            "high": [101.0, 101.0, 101.0],
            "low": [99.0, 99.0, 99.0],
            "close": [100.0, 100.0, 100.0],
            "volume": [1.0, 1.0, 1.0],
        },
        index=idx,
    )


def _register_plugin(builder) -> str:
    module_name = f"tests.fake_strategy_plugin_{uuid.uuid4().hex}"
    module = types.ModuleType(module_name)
    module.build_strategy = builder
    sys.modules[module_name] = module
    return module_name


def test_user_strategy_starter_returns_noop_signal_frame() -> None:
    df_raw = _market_df()
    cfg = {
        "strategy_plugin": {
            "module": "user_strategy",
            "callable": "build_strategy",
        },
        "strategy": {},
    }

    result = build_strategy_result(df_raw=df_raw, cfg=cfg, valid_days=set(df_raw.index.date))

    assert result.df_sig.index.equals(df_raw.index)
    assert result.df_sig["signal"].tolist() == [0, 0, 0]
    assert result.df_sig["signal_type"].tolist() == ["", "", ""]
    assert result.execution_specs == {}
    assert result.strategy_metadata == {"status": "starter_noop"}


def test_nonzero_signal_requires_signal_type() -> None:
    df_raw = _market_df()

    def _builder(**kwargs):
        _ = kwargs
        df_sig = df_raw.copy()
        df_sig["signal"] = [1, 0, 0]
        df_sig["signal_type"] = ["", "", ""]
        df_sig["orb_high"] = [110.0, 110.0, 110.0]
        return StrategyBuildResult(
            df_sig=df_sig,
            execution_specs=build_test_execution_specs(),
            strategy_metadata={},
        )

    module_name = _register_plugin(_builder)
    cfg = {"strategy_plugin": {"module": module_name, "callable": "build_strategy"}, "strategy": {}}

    with pytest.raises(ValueError, match="missing signal_type"):
        build_strategy_result(df_raw=df_raw, cfg=cfg, valid_days=set(df_raw.index.date))


def test_signal_sign_must_match_execution_side() -> None:
    df_raw = _market_df()

    def _builder(**kwargs):
        _ = kwargs
        df_sig = df_raw.copy()
        df_sig["signal"] = [1, 0, 0]
        df_sig["signal_type"] = [SHORT_ENTRY_SIGNAL, "", ""]
        df_sig["orb_high"] = [110.0, 110.0, 110.0]
        return StrategyBuildResult(
            df_sig=df_sig,
            execution_specs=build_test_execution_specs(),
            strategy_metadata={},
        )

    module_name = _register_plugin(_builder)
    cfg = {"strategy_plugin": {"module": module_name, "callable": "build_strategy"}, "strategy": {}}

    with pytest.raises(ValueError, match="positive signal"):
        build_strategy_result(df_raw=df_raw, cfg=cfg, valid_days=set(df_raw.index.date))


def test_required_orb_values_must_be_present() -> None:
    df_raw = _market_df()

    def _builder(**kwargs):
        _ = kwargs
        df_sig = df_raw.copy()
        df_sig["signal"] = [-1, 0, 0]
        df_sig["signal_type"] = [SHORT_ORB_SIGNAL, "", ""]
        df_sig["orb_high"] = [110.0, 110.0, 110.0]
        return StrategyBuildResult(
            df_sig=df_sig,
            execution_specs=build_test_execution_specs(),
            strategy_metadata={},
        )

    module_name = _register_plugin(_builder)
    cfg = {"strategy_plugin": {"module": module_name, "callable": "build_strategy"}, "strategy": {}}

    with pytest.raises(ValueError, match="requires column 'orb_low'"):
        build_strategy_result(df_raw=df_raw, cfg=cfg, valid_days=set(df_raw.index.date))


def test_missing_execution_spec_is_rejected() -> None:
    df_raw = _market_df()

    def _builder(**kwargs):
        _ = kwargs
        df_sig = df_raw.copy()
        df_sig["signal"] = [1, 0, 0]
        df_sig["signal_type"] = [LONG_ENTRY_SIGNAL, "", ""]
        df_sig["orb_high"] = [110.0, 110.0, 110.0]
        return StrategyBuildResult(
            df_sig=df_sig,
            execution_specs={},
            strategy_metadata={},
        )

    module_name = _register_plugin(_builder)
    cfg = {"strategy_plugin": {"module": module_name, "callable": "build_strategy"}, "strategy": {}}

    with pytest.raises(ValueError, match="Unsupported signal_type"):
        build_strategy_result(df_raw=df_raw, cfg=cfg, valid_days=set(df_raw.index.date))
