import os

# Determinism locks (must be set before Python does much work)
os.environ["PYTHONHASHSEED"] = "0"

import argparse
import hashlib
import json
import platform
import random
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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


def read_binance_ohlcv(file_path: Path) -> pd.DataFrame:
    df = pd.read_csv(
        file_path,
        header=None,
        skiprows=1,
        usecols=[0, 1, 2, 3, 4, 5],
        names=["timestamp", "open", "high", "low", "close", "volume"],
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).set_index("timestamp").sort_index()

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["open", "high", "low", "close"])
    return df[~df.index.duplicated(keep="first")]


def load_raw_dataset_from_manifest(
    manifest_path: Path,
    data_dir_override: Path | None,
    symbol: str,
    timeframe: str,
) -> tuple[pd.DataFrame, dict[str, Any], Path, list[str]]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    data_root = manifest.get("data_root", "")

    if data_dir_override is not None:
        data_dir = data_dir_override
    else:
        data_dir = Path(str(data_root))
        if not data_dir.is_absolute():
            data_dir = (REPO_ROOT / data_dir).resolve()

    if not data_dir.exists():
        raise FileNotFoundError(f"Raw data directory not found: {data_dir}")

    files = manifest.get("files", [])
    file_paths = [
        item.get("path")
        for item in files
        if isinstance(item, dict)
        and isinstance(item.get("path"), str)
        and item.get("path", "").lower().endswith(".csv")
    ]

    prefix = f"{symbol}-{timeframe}-"
    selected = [path for path in file_paths if path.startswith(prefix)]
    if not selected:
        selected = file_paths
    selected = sorted(selected, key=lambda value: value.lower())
    if not selected:
        raise RuntimeError("No CSV files found in manifest.")

    parts = []
    for rel in selected:
        file_path = data_dir / rel
        if not file_path.exists():
            raise FileNotFoundError(f"Missing file listed in manifest: {file_path}")
        parts.append(read_binance_ohlcv(file_path))

    df = pd.concat(parts).sort_index()
    return df[~df.index.duplicated(keep="first")], manifest, data_dir, selected


def compute_max_drawdown_pct(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    roll_max = equity.cummax()
    dd = (equity / roll_max) - 1.0
    return float(dd.min() * 100.0)


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


def maybe_write_equity_plot(equity_df: pd.DataFrame, out_path: Path) -> None:
    try:
        import contextlib
        import io

        with contextlib.redirect_stderr(io.StringIO()):
            import matplotlib

            matplotlib.use("Agg", force=True)
            import matplotlib.pyplot as plt  # noqa: F401

        with contextlib.redirect_stderr(io.StringIO()):
            plt.figure()
            plt.plot(equity_df["timestamp"], equity_df["equity"])
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(out_path)
            plt.close()
    except Exception:
        return


def _plugin_descriptor(cfg: dict[str, Any]) -> dict[str, str]:
    plugin_cfg = cfg.get("strategy_plugin") or {}
    if not isinstance(plugin_cfg, dict):
        return {}
    return {
        "module": str(plugin_cfg.get("module") or "user_strategy"),
        "callable": str(plugin_cfg.get("callable") or "build_strategy"),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Run a deterministic ORB-platform backtest")
    ap.add_argument("--config", default="config.yaml", help="Path to YAML config")
    ap.add_argument("--manifest", default="data/manifest.json", help="Path to raw manifest JSON")
    ap.add_argument("--data-dir", default="", help="Optional raw data directory override")
    ap.add_argument("--valid-days", default="data/processed/valid_days.csv", help="Path to valid_days.csv")
    ap.add_argument("--out-dir", default="reports/baseline", help="Output directory")
    ap.add_argument("--fee-mult", type=float, default=1.0)
    ap.add_argument("--slippage-bps", type=float, default=0.0)
    ap.add_argument("--delay-bars", type=int, default=1)
    ap.add_argument("--engine", choices=["spot", "futures"], default="spot")
    ap.add_argument("--leverage", type=float, default=1.0)
    ap.add_argument("--mmr", type=float, default=0.005)
    ap.add_argument("--funding-per-8h", type=float, default=0.0001)
    args = ap.parse_args()

    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = (REPO_ROOT / config_path).resolve()

    manifest_path = Path(args.manifest)
    if not manifest_path.is_absolute():
        manifest_path = (REPO_ROOT / manifest_path).resolve()

    valid_days_path = Path(args.valid_days)
    if not valid_days_path.is_absolute():
        valid_days_path = (REPO_ROOT / valid_days_path).resolve()

    out_dir = Path(args.out_dir)
    if not out_dir.is_absolute():
        out_dir = (REPO_ROOT / out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    data_dir_override = Path(args.data_dir).resolve() if args.data_dir else None

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
    valid_days = load_valid_days_csv(valid_days_path)

    processed_path = REPO_ROOT / "data" / "processed" / f"{symbol}_{timeframe}.parquet"
    used_files: list[str] = []
    raw_manifest: dict[str, Any] = {}
    data_dir: Path

    if processed_path.exists():
        df_raw = pd.read_parquet(processed_path)
        used_files = [str(processed_path)]
        data_dir = processed_path.parent
        if manifest_path.exists():
            raw_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        print(f"[OK] Loaded candles: {len(df_raw)} ({symbol} {timeframe}) [processed parquet]")
    else:
        df_raw, raw_manifest, data_dir, used_files = load_raw_dataset_from_manifest(
            manifest_path=manifest_path,
            data_dir_override=data_dir_override,
            symbol=symbol,
            timeframe=timeframe,
        )
        print(f"[OK] Loaded candles: {len(df_raw)} ({symbol} {timeframe}) [raw CSVs]")

    strategy_result = build_strategy_result(df_raw=df_raw, cfg=cfg, valid_days=valid_days)
    df_sig = strategy_result.df_sig

    total_fees_paid = 0.0
    total_funding = 0.0
    liquidations = 0
    engine_stats: dict[str, Any] = {}

    if args.engine == "spot":
        from backtester.spot_engine import backtest_orb_strategy  # type: ignore

        trades, equity_curve, final_capital, total_fees_paid = backtest_orb_strategy(
            df=df_sig,
            orb_ranges=None,
            execution_specs=strategy_result.execution_specs,
            initial_capital=initial_capital,
            position_size=position_size,
            taker_fee_rate=taker_fee_rate,
            valid_days=valid_days,
            fee_mult=float(args.fee_mult),
            slippage_bps=float(args.slippage_bps),
            delay_bars=int(args.delay_bars),
        )
        final_equity = float(equity_curve[-1]) if equity_curve else float(initial_capital)
        engine_stats = {
            "final_equity": final_equity,
            "final_capital": float(final_capital),
            "total_fees": float(total_fees_paid),
            "total_funding": 0.0,
            "liquidations": 0,
        }
    else:
        engine_cfg = FuturesEngineConfig(
            initial_capital=initial_capital,
            position_size=position_size,
            leverage=float(args.leverage),
            taker_fee_rate=taker_fee_rate,
            fee_mult=float(args.fee_mult),
            slippage_bps=float(args.slippage_bps),
            delay_bars=int(args.delay_bars),
            maintenance_margin_rate=float(args.mmr),
            funding_rate_per_8h=float(args.funding_per_8h),
        )
        trades, equity_curve, stats = backtest_futures_orb(
            df=df_sig,
            orb_ranges=None,
            execution_specs=strategy_result.execution_specs,
            valid_days=valid_days,
            cfg=engine_cfg,
            risk_limits=risk_limits,
        )
        total_fees_paid = float(stats.get("total_fees", 0.0))
        total_funding = float(stats.get("total_funding", 0.0))
        liquidations = int(stats.get("liquidations", 0))
        engine_stats = stats

    trades_df = pd.DataFrame(trades)
    equity_df = pd.DataFrame({"timestamp": df_sig.index, "equity": equity_curve})
    total_trades = int(len(trades_df))

    if total_trades:
        pnl_col = "pnl_net" if "pnl_net" in trades_df.columns else ("pnl" if "pnl" in trades_df.columns else None)
        if pnl_col:
            winning_trades = int((trades_df[pnl_col] > 0).sum())
            losing_trades = int((trades_df[pnl_col] <= 0).sum())
            win_rate = (winning_trades / total_trades) * 100.0
            avg_win = float(trades_df.loc[trades_df[pnl_col] > 0, pnl_col].mean()) if winning_trades else 0.0
            avg_loss = float(trades_df.loc[trades_df[pnl_col] <= 0, pnl_col].mean()) if losing_trades else 0.0
            total_pnl_net = float(trades_df[pnl_col].sum())
        else:
            winning_trades = 0
            losing_trades = 0
            win_rate = 0.0
            avg_win = 0.0
            avg_loss = 0.0
            total_pnl_net = 0.0
        signal_type_counts = (
            trades_df["signal_type"].value_counts().to_dict() if "signal_type" in trades_df.columns else {}
        )
    else:
        winning_trades = 0
        losing_trades = 0
        win_rate = 0.0
        avg_win = 0.0
        avg_loss = 0.0
        total_pnl_net = 0.0
        signal_type_counts = {}

    final_equity = float(equity_df["equity"].iloc[-1]) if not equity_df.empty else float(initial_capital)
    total_return_pct = (final_equity / initial_capital - 1.0) * 100.0 if initial_capital else 0.0
    max_dd_pct = compute_max_drawdown_pct(equity_df["equity"])

    results_path = out_dir / "results.json"
    trades_path = out_dir / "trades.csv"
    equity_path = out_dir / "equity_curve.csv"
    meta_path = out_dir / "run_metadata.json"
    hashes_path = out_dir / "hashes.json"
    plot_path = out_dir / "equity_curve.png"

    trades_df.to_csv(trades_path, index=False)
    equity_df.to_csv(equity_path, index=False)
    maybe_write_equity_plot(equity_df, plot_path)

    results = {
        "symbol": symbol,
        "timeframe": timeframe,
        "engine": args.engine,
        "range": {
            "start": df_sig.index.min().isoformat(),
            "end": df_sig.index.max().isoformat(),
            "candles": int(len(df_sig)),
        },
        "dataset": {
            "data_dir": str(data_dir),
            "manifest_path": str(manifest_path) if manifest_path.exists() else None,
            "raw_dataset_sha256": raw_manifest.get("dataset_sha256"),
            "processed_parquet_path": str(processed_path) if processed_path.exists() else None,
            "processed_parquet_sha256": sha256_file(processed_path) if processed_path.exists() else None,
        },
        "params": {
            "strategy_plugin": _plugin_descriptor(cfg),
            "strategy_metadata": dict(strategy_result.strategy_metadata),
            "initial_capital": initial_capital,
            "position_size": position_size,
            "taker_fee_rate": taker_fee_rate,
            "fee_mult": float(args.fee_mult),
            "slippage_bps": float(args.slippage_bps),
            "delay_bars": int(args.delay_bars),
            "leverage": float(args.leverage),
            "mmr": float(args.mmr),
            "funding_per_8h": float(args.funding_per_8h),
            "risk_controls": risk_limits.to_dict() if hasattr(risk_limits, "to_dict") else {},
        },
        "signal_type_counts": signal_type_counts,
        "metrics": {
            "Initial Capital": float(initial_capital),
            "Final Equity": float(final_equity),
            "Total Return %": float(total_return_pct),
            "Max Drawdown %": float(max_dd_pct),
            "Total Trades": total_trades,
            "Winning Trades": winning_trades,
            "Losing Trades": losing_trades,
            "Win Rate %": float(win_rate),
            "Average Win": float(avg_win),
            "Average Loss": float(avg_loss),
            "Total P&L Net": float(total_pnl_net),
            "Total Fees Paid": float(total_fees_paid),
            "Total Funding Paid": float(total_funding),
            "Liquidations": int(liquidations),
        },
        "engine_stats": engine_stats,
    }
    results_path.write_text(stable_json(results), encoding="utf-8")

    outputs = {
        "results.json": str(results_path),
        "trades.csv": str(trades_path),
        "equity_curve.csv": str(equity_path),
        "run_metadata.json": str(meta_path),
        "hashes.json": str(hashes_path),
    }
    if plot_path.exists():
        outputs["equity_curve.png"] = str(plot_path)

    hashes = {name: sha256_file(Path(path)) for name, path in outputs.items() if Path(path).exists()}
    script_path = Path(__file__).resolve()
    meta = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "git": get_git_info(),
        "python": {"version": sys.version, "executable": sys.executable},
        "platform": {"platform": platform.platform()},
        "inputs": {
            "config_path": str(config_path),
            "config_sha256": sha256_bytes(cfg_text.encode("utf-8")),
            "raw_manifest_path": str(manifest_path),
            "raw_manifest_sha256": sha256_file(manifest_path) if manifest_path.exists() else None,
            "raw_dataset_sha256": raw_manifest.get("dataset_sha256"),
            "raw_data_dir": str(data_dir),
            "valid_days_path": str(valid_days_path),
            "valid_days_sha256": sha256_file(valid_days_path),
            "used_files": used_files,
            "script_path": str(script_path),
            "script_sha256": sha256_file(script_path),
            "engine": args.engine,
        },
        "outputs": outputs,
        "output_sha256": hashes,
    }
    meta_path.write_text(stable_json(meta), encoding="utf-8")
    hashes_path.write_text(stable_json(hashes), encoding="utf-8")

    print(f"\n[OK] Wrote outputs to: {out_dir}")
    for name in outputs:
        print(f"  - {name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
