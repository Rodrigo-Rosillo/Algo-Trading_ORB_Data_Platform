from __future__ import annotations

from typing import Any

import pandas as pd

from core.strategy_plugin import StrategyBuildResult


def build_strategy(
    *,
    df_raw: pd.DataFrame,
    cfg: dict[str, Any],
    strategy_config: dict[str, Any],
    valid_days: set | None,
) -> StrategyBuildResult:
    """Starter strategy plug-in.

    Replace this function with your own signal-generation logic.

    Required output:
      - df_sig indexed exactly like df_raw
      - df_sig["signal"] with negative/zero/positive integers
      - df_sig["signal_type"] with a non-empty label only when signal != 0
      - execution_specs entries for every signal_type you emit
      - orb_high/orb_low columns when your execution specs require them
    """
    _ = (cfg, strategy_config, valid_days)

    df_sig = pd.DataFrame(index=df_raw.index)
    df_sig["signal"] = 0
    df_sig["signal_type"] = ""

    return StrategyBuildResult(
        df_sig=df_sig,
        execution_specs={},
        strategy_metadata={"status": "starter_noop"},
    )
