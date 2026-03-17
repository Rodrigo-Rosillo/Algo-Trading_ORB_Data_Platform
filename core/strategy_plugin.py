from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import importlib
import json
from typing import Any

import numpy as np
import pandas as pd

from execution_specs import ExecutionSpec, get_execution_spec, required_orb_fields


@dataclass(frozen=True)
class StrategyBuildResult:
    df_sig: pd.DataFrame
    execution_specs: dict[str, ExecutionSpec]
    strategy_metadata: dict[str, Any]


def _strategy_plugin_cfg(cfg: Mapping[str, Any]) -> tuple[str, str]:
    plugin_cfg = cfg.get("strategy_plugin") or {}
    if not isinstance(plugin_cfg, Mapping):
        raise ValueError("strategy_plugin must be a mapping")

    module_name = str(plugin_cfg.get("module") or "user_strategy").strip()
    callable_name = str(plugin_cfg.get("callable") or "build_strategy").strip()
    if not module_name:
        raise ValueError("strategy_plugin.module must be a non-empty string")
    if not callable_name:
        raise ValueError("strategy_plugin.callable must be a non-empty string")
    return module_name, callable_name


def load_strategy_builder(cfg: Mapping[str, Any]) -> Callable[..., StrategyBuildResult]:
    module_name, callable_name = _strategy_plugin_cfg(cfg)
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        raise ImportError(f"Unable to import strategy plug-in module {module_name!r}") from exc

    builder = getattr(module, callable_name, None)
    if not callable(builder):
        raise TypeError(
            f"strategy plug-in callable {callable_name!r} was not found or is not callable in module {module_name!r}"
        )
    return builder


def _normalize_execution_specs(raw_specs: Any) -> dict[str, ExecutionSpec]:
    if raw_specs is None:
        return {}
    if not isinstance(raw_specs, Mapping):
        raise TypeError("StrategyBuildResult.execution_specs must be a mapping of signal_type -> ExecutionSpec")

    out: dict[str, ExecutionSpec] = {}
    for raw_key, raw_value in raw_specs.items():
        key = str(raw_key).strip()
        if not key:
            raise ValueError("execution_specs keys must be non-empty signal_type strings")
        if not isinstance(raw_value, ExecutionSpec):
            raise TypeError(
                f"execution_specs[{key!r}] must be an ExecutionSpec instance, got {type(raw_value).__name__}"
            )
        out[key] = raw_value
    return out


def _normalize_signal_column(signal_series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(signal_series, errors="raise")
    values = numeric.to_numpy(dtype=float, copy=False)
    finite_mask = np.isfinite(values)
    if not finite_mask.all():
        raise ValueError("Plug-in signal column must contain only finite numeric values")

    rounded = np.rint(values)
    if not np.allclose(values, rounded):
        raise ValueError("Plug-in signal column must contain only integer-like values")

    return pd.Series(rounded.astype(int), index=signal_series.index, name="signal")


def _validate_strategy_metadata(metadata: Any) -> dict[str, Any]:
    if metadata is None:
        return {}
    if not isinstance(metadata, dict):
        raise TypeError("StrategyBuildResult.strategy_metadata must be a JSON-serializable dict")
    try:
        json.dumps(metadata, sort_keys=True)
    except TypeError as exc:
        raise TypeError("StrategyBuildResult.strategy_metadata must be JSON-serializable") from exc
    return dict(metadata)


def _base_signal_frame(df_raw: pd.DataFrame) -> pd.DataFrame:
    out = df_raw.copy()
    if not isinstance(out.index, pd.DatetimeIndex):
        raise TypeError("Strategy input data must be indexed by a pandas DatetimeIndex")
    out = out.sort_index()
    out["date"] = out.index.date
    return out


def _validate_signals_and_specs(df_sig: pd.DataFrame, execution_specs: Mapping[str, ExecutionSpec]) -> None:
    nonzero = df_sig.loc[df_sig["signal"] != 0].copy()
    if nonzero.empty:
        return

    for ts, row in nonzero.iterrows():
        signal_type = str(row.get("signal_type", "") or "").strip()
        if not signal_type:
            raise ValueError(f"Non-zero signal at {ts} is missing signal_type")

        spec = get_execution_spec(execution_specs, signal_type)
        signal_value = int(row["signal"])
        if signal_value > 0 and spec.side != "long":
            raise ValueError(
                f"Signal at {ts} has positive signal={signal_value} but execution spec side={spec.side!r}"
            )
        if signal_value < 0 and spec.side != "short":
            raise ValueError(
                f"Signal at {ts} has negative signal={signal_value} but execution spec side={spec.side!r}"
            )

        for field_name in required_orb_fields(spec):
            if field_name not in df_sig.columns:
                raise ValueError(
                    f"Signal {signal_type!r} at {ts} requires column {field_name!r}, but the plug-in did not provide it"
                )
            field_value = row.get(field_name)
            if pd.isna(field_value):
                raise ValueError(
                    f"Signal {signal_type!r} at {ts} requires non-null {field_name!r} for bracket calculation"
                )


def normalize_strategy_build_result(
    *,
    df_raw: pd.DataFrame,
    result: StrategyBuildResult,
) -> StrategyBuildResult:
    if not isinstance(result, StrategyBuildResult):
        raise TypeError(
            "Strategy plug-in must return core.strategy_plugin.StrategyBuildResult"
        )

    plugin_df = result.df_sig
    if not isinstance(plugin_df, pd.DataFrame):
        raise TypeError("StrategyBuildResult.df_sig must be a pandas DataFrame")
    if not isinstance(plugin_df.index, pd.DatetimeIndex):
        raise TypeError("StrategyBuildResult.df_sig must use a pandas DatetimeIndex")
    if not plugin_df.index.equals(df_raw.index):
        raise ValueError("StrategyBuildResult.df_sig must be aligned exactly to the input market-data index")
    if "signal" not in plugin_df.columns:
        raise ValueError("StrategyBuildResult.df_sig must include a signal column")
    if "signal_type" not in plugin_df.columns:
        raise ValueError("StrategyBuildResult.df_sig must include a signal_type column")

    execution_specs = _normalize_execution_specs(result.execution_specs)
    strategy_metadata = _validate_strategy_metadata(result.strategy_metadata)

    normalized = _base_signal_frame(df_raw)
    for column in plugin_df.columns:
        normalized[column] = plugin_df[column]

    normalized["signal"] = _normalize_signal_column(normalized["signal"])
    normalized["signal_type"] = normalized["signal_type"].astype(str).fillna("")
    normalized.loc[normalized["signal"] == 0, "signal_type"] = ""

    _validate_signals_and_specs(normalized, execution_specs)

    return StrategyBuildResult(
        df_sig=normalized,
        execution_specs=execution_specs,
        strategy_metadata=strategy_metadata,
    )


def build_strategy_result(
    *,
    df_raw: pd.DataFrame,
    cfg: Mapping[str, Any],
    valid_days: set | None,
) -> StrategyBuildResult:
    builder = load_strategy_builder(cfg)
    strategy_cfg = cfg.get("strategy") or {}
    if strategy_cfg is None:
        strategy_cfg = {}
    if not isinstance(strategy_cfg, Mapping):
        raise ValueError("strategy must be a mapping when provided")

    result = builder(
        df_raw=df_raw.copy(),
        cfg=cfg,
        strategy_config=dict(strategy_cfg),
        valid_days=None if valid_days is None else set(valid_days),
    )
    return normalize_strategy_build_result(df_raw=df_raw, result=result)
