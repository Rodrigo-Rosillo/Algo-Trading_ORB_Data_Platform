import os

# Determinism locks
os.environ["PYTHONHASHSEED"] = "0"

import argparse
import hashlib
import platform
import random
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd
import yaml

# Determinism: RNG seeds
random.seed(0)
np.random.seed(0)

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backtester.futures_engine import FuturesEngineConfig, backtest_futures_orb  # noqa: E402
from backtester.risk import risk_limits_from_config  # noqa: E402
from core.strategy_plugin import build_strategy_result  # noqa: E402
from core.utils import load_valid_days_csv, sha256_file, stable_json  # noqa: E402


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def compute_max_drawdown_pct(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    roll_max = equity.cummax()
    dd = (equity / roll_max) - 1.0
    return float(dd.min() * 100.0)


def compute_daily_sharpe(equity_df: pd.DataFrame) -> float:
    if equity_df.empty:
        return 0.0
    series = equity_df.set_index("timestamp")["equity"]
    daily = series.resample("1D").last().dropna()
    rets = daily.pct_change().dropna()
    if len(rets) < 2:
        return 0.0
    std = float(rets.std(ddof=1))
    if std == 0.0 or np.isnan(std):
        return 0.0
    return float((rets.mean() / std) * np.sqrt(365.0))


def compute_cagr(equity_df: pd.DataFrame, initial_capital: float) -> float:
    if equity_df.empty or initial_capital <= 0:
        return 0.0
    start_ts = pd.to_datetime(equity_df["timestamp"].iloc[0], utc=True)
    end_ts = pd.to_datetime(equity_df["timestamp"].iloc[-1], utc=True)
    days = (end_ts - start_ts).total_seconds() / 86400.0
    if days <= 0:
        return 0.0
    years = days / 365.0
    final_equity = float(equity_df["equity"].iloc[-1])
    return float((final_equity / initial_capital) ** (1.0 / years) - 1.0)


def summarize_run(
    trades_df: pd.DataFrame,
    equity_df: pd.DataFrame,
    initial_capital: float,
    total_fees: float,
    total_funding: float,
    liquidations: int,
) -> dict[str, Any]:
    total_trades = int(len(trades_df))
    pnl_col = "pnl_net" if "pnl_net" in trades_df.columns else ("pnl" if "pnl" in trades_df.columns else None)

    if total_trades and pnl_col:
        pnl = trades_df[pnl_col]
        wins = int((pnl > 0).sum())
        losses = int((pnl <= 0).sum())
        win_rate = (wins / total_trades) * 100.0
        avg_win = float(pnl[pnl > 0].mean()) if wins else 0.0
        avg_loss = float(pnl[pnl <= 0].mean()) if losses else 0.0
        total_pnl_net = float(pnl.sum())
        expectancy = float(total_pnl_net / total_trades)
    else:
        wins = 0
        losses = 0
        win_rate = 0.0
        avg_win = 0.0
        avg_loss = 0.0
        total_pnl_net = 0.0
        expectancy = 0.0

    final_equity = float(equity_df["equity"].iloc[-1]) if not equity_df.empty else float(initial_capital)
    total_return_pct = (final_equity / initial_capital - 1.0) * 100.0 if initial_capital else 0.0

    return {
        "initial_capital": float(initial_capital),
        "final_equity": float(final_equity),
        "total_return_pct": float(total_return_pct),
        "cagr": float(compute_cagr(equity_df, initial_capital)),
        "max_drawdown_pct": float(compute_max_drawdown_pct(equity_df["equity"])) if not equity_df.empty else 0.0,
        "daily_sharpe": float(compute_daily_sharpe(equity_df)),
        "total_trades": total_trades,
        "winning_trades": wins,
        "losing_trades": losses,
        "win_rate_pct": float(win_rate),
        "avg_win": float(avg_win),
        "avg_loss": float(avg_loss),
        "total_pnl_net": float(total_pnl_net),
        "expectancy_per_trade": float(expectancy),
        "total_fees": float(total_fees),
        "total_funding": float(total_funding),
        "liquidations": int(liquidations),
    }


def generate_folds(
    start_utc: pd.Timestamp,
    end_utc: pd.Timestamp,
    train_months: int,
    test_months: int,
    step_months: int,
) -> list[dict[str, pd.Timestamp]]:
    folds: list[dict[str, pd.Timestamp]] = []
    cursor = start_utc.normalize()

    while True:
        train_start = cursor
        train_end = train_start + pd.DateOffset(months=train_months)
        test_start = train_end
        test_end = test_start + pd.DateOffset(months=test_months)
        if test_end > end_utc:
            break
        folds.append(
            {
                "train_start": train_start,
                "train_end": train_end,
                "test_start": test_start,
                "test_end": test_end,
            }
        )
        cursor = cursor + pd.DateOffset(months=step_months)

    return folds


def get_git_info() -> dict[str, Any]:
    def run(cmd: list[str]) -> str:
        return subprocess.check_output(cmd, cwd=str(REPO_ROOT), stderr=subprocess.DEVNULL).decode().strip()

    info: dict[str, Any] = {}
    try:
        info["commit"] = run(["git", "rev-parse", "HEAD"])
        info["branch"] = run(["git", "rev-parse", "--abbrev-ref", "HEAD"])
        info["dirty"] = bool(run(["git", "status", "--porcelain"]))
    except Exception:
        info["commit"] = None
        info["branch"] = None
        info["dirty"] = None
    return info


def _plugin_descriptor(cfg: dict[str, Any]) -> dict[str, str]:
    plugin_cfg = cfg.get("strategy_plugin") or {}
    if not isinstance(plugin_cfg, dict):
        return {}
    return {
        "module": str(plugin_cfg.get("module") or "user_strategy"),
        "callable": str(plugin_cfg.get("callable") or "build_strategy"),
    }


@dataclass(frozen=True)
class WalkForwardRunConfig:
    config: str | Path = "config.yaml"
    data: str | Path = "data/processed/BTCUSDT_30m.parquet"
    valid_days: str | Path = "data/processed/valid_days.csv"
    out_dir: str | Path = "reports/walk_forward"
    engine: str = "futures"
    train_months: int = 24
    test_months: int = 6
    step_months: int = 6
    start: str = ""
    end: str = ""
    fee_mult: float = 1.0
    slippage_bps: float = 0.0
    delay_bars: int = 1
    leverage: float = 1.0
    mmr: float = 0.005
    funding_per_8h: float = 0.0001


@dataclass(frozen=True)
class WalkForwardRunResult:
    out_dir: Path
    folds_csv: Path
    report_html: Path
    metadata_json: Path
    folds_dir: Path
    folds: int
    aggregate: dict[str, Any]


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Run walk-forward evaluation for an ORB strategy plug-in")
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--data", default="data/processed/BTCUSDT_30m.parquet", help="Processed parquet dataset")
    ap.add_argument("--valid-days", default="data/processed/valid_days.csv")
    ap.add_argument("--out-dir", default="reports/walk_forward")
    ap.add_argument("--engine", choices=["futures", "spot"], default="futures")
    ap.add_argument("--train-months", type=int, default=24)
    ap.add_argument("--test-months", type=int, default=6)
    ap.add_argument("--step-months", type=int, default=6)
    ap.add_argument("--start", default="", help="Optional UTC start timestamp/date")
    ap.add_argument("--end", default="", help="Optional UTC end timestamp/date")
    ap.add_argument("--fee-mult", type=float, default=1.0)
    ap.add_argument("--slippage-bps", type=float, default=0.0)
    ap.add_argument("--delay-bars", type=int, default=1)
    ap.add_argument("--leverage", type=float, default=1.0)
    ap.add_argument("--mmr", type=float, default=0.005)
    ap.add_argument("--funding-per-8h", type=float, default=0.0001)
    return ap


def parse_run_config(argv: Sequence[str] | None = None) -> WalkForwardRunConfig:
    args = build_arg_parser().parse_args(argv)
    return WalkForwardRunConfig(
        config=args.config,
        data=args.data,
        valid_days=args.valid_days,
        out_dir=args.out_dir,
        engine=args.engine,
        train_months=int(args.train_months),
        test_months=int(args.test_months),
        step_months=int(args.step_months),
        start=str(args.start),
        end=str(args.end),
        fee_mult=float(args.fee_mult),
        slippage_bps=float(args.slippage_bps),
        delay_bars=int(args.delay_bars),
        leverage=float(args.leverage),
        mmr=float(args.mmr),
        funding_per_8h=float(args.funding_per_8h),
    )


def run_walk_forward(run_cfg: WalkForwardRunConfig) -> WalkForwardRunResult:
    config_path = Path(run_cfg.config)
    if not config_path.is_absolute():
        config_path = (REPO_ROOT / config_path).resolve()

    data_path = Path(run_cfg.data)
    if not data_path.is_absolute():
        data_path = (REPO_ROOT / data_path).resolve()

    valid_days_path = Path(run_cfg.valid_days)
    if not valid_days_path.is_absolute():
        valid_days_path = (REPO_ROOT / valid_days_path).resolve()

    out_dir = Path(run_cfg.out_dir)
    if not out_dir.is_absolute():
        out_dir = (REPO_ROOT / out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    folds_dir = out_dir / "folds"
    folds_dir.mkdir(parents=True, exist_ok=True)

    cfg_text = config_path.read_text(encoding="utf-8")
    cfg = yaml.safe_load(cfg_text) or {}
    if not isinstance(cfg, dict):
        raise ValueError("Config must be a YAML mapping")

    symbol = str(cfg.get("symbol", "BTCUSDT"))
    timeframe = str(cfg.get("timeframe", "30m"))
    initial_capital = float(cfg["risk"]["initial_capital"])
    position_size = float(cfg["risk"]["position_size"])
    taker_fee_rate = float(cfg["fees"]["taker_fee_rate"])
    risk_limits = risk_limits_from_config(cfg)
    valid_days_all = load_valid_days_csv(valid_days_path)

    df = pd.read_parquet(data_path)
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("Parquet dataset must have a DatetimeIndex")
    if df.index.tz is None:
        df = df.tz_localize("UTC")

    needed_cols = [col for col in ["open", "high", "low", "close", "volume"] if col in df.columns]
    df = df[needed_cols].copy().sort_index()

    start_ts = pd.to_datetime(run_cfg.start, utc=True) if run_cfg.start else df.index.min()
    end_ts = pd.to_datetime(run_cfg.end, utc=True) if run_cfg.end else df.index.max()
    folds = generate_folds(start_ts, end_ts, run_cfg.train_months, run_cfg.test_months, run_cfg.step_months)
    if not folds:
        raise RuntimeError("No folds generated. Try smaller train/test months or adjust --start/--end.")

    spot_engine = None
    if run_cfg.engine == "spot":
        from backtester.spot_engine import backtest_orb_strategy  # type: ignore

        spot_engine = backtest_orb_strategy

    summary_rows: list[dict[str, Any]] = []

    for idx, fold in enumerate(folds, start=1):
        fold_id = f"fold_{idx:02d}"
        fold_out = folds_dir / fold_id
        fold_out.mkdir(parents=True, exist_ok=True)

        test_start = fold["test_start"]
        test_end = fold["test_end"]
        df_test = df[(df.index >= test_start) & (df.index < test_end)].copy()
        test_days = set(pd.Series(df_test.index.normalize().date).unique())
        valid_test_days = valid_days_all.intersection(test_days)

        strategy_result = build_strategy_result(df_raw=df_test, cfg=cfg, valid_days=valid_test_days)
        df_sig = strategy_result.df_sig

        total_fees = 0.0
        total_funding = 0.0
        liquidations = 0
        engine_stats: dict[str, Any] = {}

        if run_cfg.engine == "futures":
            engine_cfg = FuturesEngineConfig(
                initial_capital=initial_capital,
                position_size=position_size,
                leverage=float(run_cfg.leverage),
                taker_fee_rate=taker_fee_rate,
                fee_mult=float(run_cfg.fee_mult),
                slippage_bps=float(run_cfg.slippage_bps),
                delay_bars=int(run_cfg.delay_bars),
                maintenance_margin_rate=float(run_cfg.mmr),
                funding_rate_per_8h=float(run_cfg.funding_per_8h),
            )
            trades, equity_curve, stats = backtest_futures_orb(
                df=df_sig,
                orb_ranges=None,
                execution_specs=strategy_result.execution_specs,
                valid_days=valid_test_days,
                cfg=engine_cfg,
                risk_limits=risk_limits,
            )
            total_fees = float(stats.get("total_fees", 0.0))
            total_funding = float(stats.get("total_funding", 0.0))
            liquidations = int(stats.get("liquidations", 0))
            engine_stats = stats
        else:
            assert spot_engine is not None
            trades, equity_curve, _, total_fees = spot_engine(
                df=df_sig,
                orb_ranges=None,
                execution_specs=strategy_result.execution_specs,
                initial_capital=initial_capital,
                position_size=position_size,
                taker_fee_rate=taker_fee_rate,
                valid_days=valid_test_days,
                fee_mult=float(run_cfg.fee_mult),
                slippage_bps=float(run_cfg.slippage_bps),
                delay_bars=int(run_cfg.delay_bars),
            )

        trades_df = pd.DataFrame(trades)
        equity_df = pd.DataFrame({"timestamp": df_sig.index, "equity": equity_curve})
        metrics = summarize_run(trades_df, equity_df, initial_capital, total_fees, total_funding, liquidations)

        trades_df.to_csv(fold_out / "trades.csv", index=False)
        equity_df.to_csv(fold_out / "equity_curve.csv", index=False)
        (fold_out / "results.json").write_text(
            stable_json(
                {
                    "fold_id": fold_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "engine": run_cfg.engine,
                    "window": {
                        "train_start": fold["train_start"].isoformat(),
                        "train_end": fold["train_end"].isoformat(),
                        "test_start": test_start.isoformat(),
                        "test_end": test_end.isoformat(),
                    },
                    "params": {
                        "strategy_plugin": _plugin_descriptor(cfg),
                        "strategy_metadata": dict(strategy_result.strategy_metadata),
                        "fee_mult": float(run_cfg.fee_mult),
                        "slippage_bps": float(run_cfg.slippage_bps),
                        "delay_bars": int(run_cfg.delay_bars),
                        "leverage": float(run_cfg.leverage),
                        "mmr": float(run_cfg.mmr),
                        "funding_per_8h": float(run_cfg.funding_per_8h),
                    },
                    "metrics": metrics,
                    "engine_stats": engine_stats,
                }
            ),
            encoding="utf-8",
        )

        summary_rows.append(
            {
                "fold_id": fold_id,
                "train_start": fold["train_start"].date().isoformat(),
                "train_end": (fold["train_end"] - pd.Timedelta(seconds=1)).date().isoformat(),
                "test_start": test_start.date().isoformat(),
                "test_end": (test_end - pd.Timedelta(seconds=1)).date().isoformat(),
                **metrics,
                "fold_dir": str(fold_out),
            }
        )

        print(
            f"[OK] {fold_id} TEST {test_start.date()}->{(test_end - pd.Timedelta(days=1)).date()} | "
            f"ret={metrics['total_return_pct']:.2f}% dd={metrics['max_drawdown_pct']:.2f}% "
            f"sharpe={metrics['daily_sharpe']:.2f} trades={metrics['total_trades']}"
        )

    summary_df = pd.DataFrame(summary_rows)
    summary_csv = out_dir / "walk_forward_folds.csv"
    summary_df.to_csv(summary_csv, index=False)

    aggregate: dict[str, Any] = {}
    if not summary_df.empty:
        for col in [
            "total_return_pct",
            "max_drawdown_pct",
            "daily_sharpe",
            "cagr",
            "total_trades",
            "win_rate_pct",
            "expectancy_per_trade",
        ]:
            if col in summary_df.columns:
                values = pd.to_numeric(summary_df[col], errors="coerce").dropna()
                if not values.empty:
                    aggregate[col] = {
                        "mean": float(values.mean()),
                        "median": float(values.median()),
                        "p25": float(values.quantile(0.25)),
                        "p75": float(values.quantile(0.75)),
                        "min": float(values.min()),
                        "max": float(values.max()),
                    }

        aggregate["folds"] = int(len(summary_df))
        aggregate["pct_folds_positive_return"] = float((summary_df["total_return_pct"] > 0).mean() * 100.0)

    metadata = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "git": get_git_info(),
        "python": {"version": sys.version, "executable": sys.executable},
        "platform": {"platform": platform.platform()},
        "inputs": {
            "config_path": str(config_path),
            "config_sha256": sha256_bytes(cfg_text.encode("utf-8")),
            "data_path": str(data_path),
            "data_sha256": sha256_file(data_path),
            "valid_days_path": str(valid_days_path),
            "valid_days_sha256": sha256_file(valid_days_path),
        },
        "params": {
            "engine": run_cfg.engine,
            "train_months": int(run_cfg.train_months),
            "test_months": int(run_cfg.test_months),
            "step_months": int(run_cfg.step_months),
            "start": run_cfg.start,
            "end": run_cfg.end,
            "fee_mult": float(run_cfg.fee_mult),
            "slippage_bps": float(run_cfg.slippage_bps),
            "delay_bars": int(run_cfg.delay_bars),
            "leverage": float(run_cfg.leverage),
            "mmr": float(run_cfg.mmr),
            "funding_per_8h": float(run_cfg.funding_per_8h),
            "strategy_plugin": _plugin_descriptor(cfg),
        },
        "outputs": {
            "folds_csv": str(summary_csv),
            "folds_dir": str(folds_dir),
        },
        "aggregate": aggregate,
    }
    metadata_path = out_dir / "walk_forward_metadata.json"
    metadata_path.write_text(stable_json(metadata), encoding="utf-8")

    plugin_desc = _plugin_descriptor(cfg)
    report_html = out_dir / "walk_forward_report.html"
    html_lines = [
        "<html><head><meta charset='utf-8'><title>Walk Forward Report</title></head><body>",
        "<h1>Walk Forward Report</h1>",
        f"<p><b>Symbol:</b> {symbol} | <b>Timeframe:</b> {timeframe} | <b>Engine:</b> {run_cfg.engine}</p>",
        f"<p><b>Plug-in:</b> {plugin_desc.get('module', '')}.{plugin_desc.get('callable', '')}</p>",
        f"<p><b>Windows:</b> train={run_cfg.train_months}m test={run_cfg.test_months}m step={run_cfg.step_months}m</p>",
        f"<p><b>Folds:</b> {aggregate.get('folds', 0)} | <b>% positive folds:</b> {aggregate.get('pct_folds_positive_return', 0):.2f}%</p>",
        "<h2>Aggregate</h2>",
        "<pre>" + stable_json(aggregate) + "</pre>",
        "<h2>Per-fold</h2>",
        "<p>See walk_forward_folds.csv and fold folders under reports/walk_forward/folds/.</p>",
        "</body></html>",
    ]
    report_html.write_text("\n".join(html_lines), encoding="utf-8")

    print(f"\n[OK] Walk-forward written to: {out_dir}")
    print("  - walk_forward_folds.csv")
    print("  - walk_forward_report.html")
    print("  - walk_forward_metadata.json")
    return WalkForwardRunResult(
        out_dir=out_dir,
        folds_csv=summary_csv,
        report_html=report_html,
        metadata_json=metadata_path,
        folds_dir=folds_dir,
        folds=int(aggregate.get("folds", 0)),
        aggregate=aggregate,
    )


def main(argv: Sequence[str] | None = None) -> int:
    run_walk_forward(parse_run_config(argv))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
