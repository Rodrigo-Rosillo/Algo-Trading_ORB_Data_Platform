"""End-to-end integration test for the full data pipeline.

Exercises the complete chain:
    hash_data → data_quality → build_parquet → run_baseline

All work is isolated inside a temporary directory with synthetic Binance-
format CSV data and an ephemeral strategy plug-in.
"""
from __future__ import annotations

import json
import sys
import types
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from core.strategy_plugin import StrategyBuildResult
from execution_specs import ExecutionSpec
from scripts.build_parquet import (
    compute_valid_invalid_days,
    load_dataset_from_manifest,
    read_binance_csv,
)
from scripts.data_quality import (
    build_html,
    is_aligned,
    read_binance_timestamps_only,
    summarize_gaps,
)
from scripts.hash_data import build_manifest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SIGNAL_TYPE = "long_orb_breakout"

EXECUTION_SPECS = {
    SIGNAL_TYPE: ExecutionSpec(
        side="long",
        target_kind="orb_high",
        stop_kind="symmetric_to_target",
    ),
}


def _write_synthetic_csv(path: Path, start_date: str, days: int) -> None:
    """Write a Binance kline-format CSV with *days* full UTC days of 30m bars.

    Each day has exactly 48 bars (00:00 – 23:30 UTC).  Prices follow a gentle
    uptrend so the long-only strategy can produce at least one winning trade.
    """
    rows: list[str] = []
    # Header row (matches Binance download format that build_parquet skips)
    rows.append(
        "open_time,open,high,low,close,volume,"
        "close_time,quote_asset_volume,count,"
        "taker_buy_volume,taker_buy_quote_volume,ignore"
    )

    base_ts = pd.Timestamp(start_date, tz="UTC")
    bar_idx = 0
    for day in range(days):
        for half_hour in range(48):
            ts = base_ts + pd.Timedelta(days=day, minutes=30 * half_hour)
            open_time_ms = int(ts.timestamp() * 1000)
            close_time_ms = open_time_ms + 30 * 60 * 1000 - 1

            # Gentle uptrend: price rises ~0.10 per bar
            base_price = 100.0 + bar_idx * 0.10
            o = round(base_price, 2)
            h = round(base_price + 1.5, 2)   # high enough for TP
            l = round(base_price - 0.5, 2)
            c = round(base_price + 0.05, 2)
            v = 10.0

            rows.append(
                f"{open_time_ms},{o},{h},{l},{c},{v},"
                f"{close_time_ms},1000,50,5.0,500,0"
            )
            bar_idx += 1

    path.write_text("\n".join(rows), encoding="utf-8")


def _register_strategy_plugin(df_raw: pd.DataFrame) -> str:
    """Register an ephemeral strategy plug-in that longs bar-0 of each day."""
    module_name = f"tests._e2e_strategy_{uuid.uuid4().hex}"
    module = types.ModuleType(module_name)

    def build_strategy(
        *,
        df_raw: pd.DataFrame,
        cfg: dict,
        strategy_config: dict,
        valid_days: set | None,
    ) -> StrategyBuildResult:
        df_sig = df_raw.copy()
        df_sig["signal"] = 0
        df_sig["signal_type"] = ""
        df_sig["orb_high"] = float("nan")
        df_sig["orb_low"] = float("nan")

        valid = valid_days or set()
        seen_days: set = set()
        for i, ts in enumerate(df_sig.index):
            d = ts.date()
            if d in valid and d not in seen_days:
                seen_days.add(d)
                df_sig.iloc[i, df_sig.columns.get_loc("signal")] = 1
                df_sig.iloc[i, df_sig.columns.get_loc("signal_type")] = SIGNAL_TYPE
                df_sig.iloc[i, df_sig.columns.get_loc("orb_high")] = float(
                    df_sig.iloc[i]["close"] + 1.0
                )
                df_sig.iloc[i, df_sig.columns.get_loc("orb_low")] = float(
                    df_sig.iloc[i]["close"] - 1.0
                )

        return StrategyBuildResult(
            df_sig=df_sig,
            execution_specs=EXECUTION_SPECS,
            strategy_metadata={"case": "e2e_pipeline"},
        )

    module.build_strategy = build_strategy  # type: ignore[attr-defined]
    sys.modules[module_name] = module
    return module_name


def _build_config(plugin_module: str) -> dict:
    return {
        "symbol": "TESTUSDT",
        "timeframe": "30m",
        "timezone": {"data_timezone": "UTC", "orb_times_timezone": "UTC"},
        "strategy_plugin": {
            "module": plugin_module,
            "callable": "build_strategy",
        },
        "strategy": {},
        "risk": {"initial_capital": 10_000, "position_size": 0.95},
        "fees": {"taker_fee_rate": 0.0005},
        "leverage": {"enabled": True, "max_leverage": 1},
        "risk_controls": {"enabled": False},
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def workspace(tmp_path: Path):
    """Set up an isolated workspace with raw data, config, and dirs."""
    raw_dir = tmp_path / "data" / "raw"
    raw_dir.mkdir(parents=True)
    processed_dir = tmp_path / "data" / "processed"
    processed_dir.mkdir(parents=True)
    reports_quality_dir = tmp_path / "reports" / "data_quality"
    reports_quality_dir.mkdir(parents=True)
    reports_baseline_dir = tmp_path / "reports" / "baseline"
    reports_baseline_dir.mkdir(parents=True)

    # 3 full UTC days → 144 bars, all valid
    csv_path = raw_dir / "TESTUSDT-30m-2024-01.csv"
    _write_synthetic_csv(csv_path, "2024-01-01", days=3)

    return {
        "root": tmp_path,
        "raw_dir": raw_dir,
        "processed_dir": processed_dir,
        "reports_quality_dir": reports_quality_dir,
        "reports_baseline_dir": reports_baseline_dir,
        "csv_path": csv_path,
    }


# ---------------------------------------------------------------------------
# Stage 1: hash_data
# ---------------------------------------------------------------------------

class TestStage1HashData:
    def test_manifest_created(self, workspace):
        manifest = build_manifest(workspace["raw_dir"], ["*.csv"])
        manifest_path = workspace["root"] / "data" / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
        )

        assert manifest_path.exists()
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert data["file_count"] == 1
        assert data["schema_version"] == 1
        assert len(data["files"]) == 1
        assert data["files"][0]["path"].endswith(".csv")
        assert len(data["dataset_sha256"]) == 64  # SHA-256 hex

    def test_manifest_hash_is_stable(self, workspace):
        m1 = build_manifest(workspace["raw_dir"], ["*.csv"])
        m2 = build_manifest(workspace["raw_dir"], ["*.csv"])
        assert m1["dataset_sha256"] == m2["dataset_sha256"]


# ---------------------------------------------------------------------------
# Stage 2: data_quality
# ---------------------------------------------------------------------------

class TestStage2DataQuality:
    def test_timestamps_readable(self, workspace):
        ts, nat_count = read_binance_timestamps_only(workspace["csv_path"])
        assert nat_count == 0
        assert len(ts) == 144  # 3 days × 48 bars

    def test_alignment(self, workspace):
        ts, _ = read_binance_timestamps_only(workspace["csv_path"])
        idx = pd.DatetimeIndex(ts).sort_values()
        aligned = is_aligned(idx, 30)
        assert aligned.all()

    def test_no_gaps(self, workspace):
        ts, _ = read_binance_timestamps_only(workspace["csv_path"])
        idx = pd.DatetimeIndex(ts).sort_values().drop_duplicates()
        gaps = summarize_gaps(idx, pd.Timedelta(minutes=30))
        assert len(gaps) == 0

    def test_html_renderable(self, workspace):
        report = {
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "symbol": "TESTUSDT",
            "dataset": {"files_analyzed": 1},
            "summary": {"bars_total": 144},
            "missing_by_day": [],
            "gaps_sample": [],
            "missing_sample": [],
            "misaligned_sample": [],
            "files": [],
        }
        html = build_html(report)
        assert "<html>" in html
        assert "Data Quality Report" in html
        assert "144" in html


# ---------------------------------------------------------------------------
# Stage 3: build_parquet
# ---------------------------------------------------------------------------

class TestStage3BuildParquet:
    def test_csv_round_trip(self, workspace):
        df = read_binance_csv(workspace["csv_path"])
        assert isinstance(df.index, pd.DatetimeIndex)
        assert len(df) == 144
        assert set(["open", "high", "low", "close", "volume"]).issubset(df.columns)

    def test_load_from_manifest(self, workspace):
        manifest = build_manifest(workspace["raw_dir"], ["*.csv"])
        df, used_files = load_dataset_from_manifest(
            workspace["raw_dir"], manifest, "TESTUSDT", "30m"
        )
        assert len(df) == 144
        assert len(used_files) == 1

    def test_valid_invalid_days(self, workspace):
        df = read_binance_csv(workspace["csv_path"])
        valid_df, invalid_df = compute_valid_invalid_days(df.index, 48)
        # All 3 days have exactly 48 bars → all valid
        assert len(valid_df) == 3
        assert len(invalid_df) == 0
        assert (valid_df["present_bars"] == 48).all()

    def test_parquet_written(self, workspace):
        manifest = build_manifest(workspace["raw_dir"], ["*.csv"])
        df, _ = load_dataset_from_manifest(
            workspace["raw_dir"], manifest, "TESTUSDT", "30m"
        )
        df = df.copy()
        df["date_utc"] = df.index.normalize().date.astype(str)

        parquet_path = workspace["processed_dir"] / "TESTUSDT_30m.parquet"
        df.to_parquet(parquet_path, index=True)
        assert parquet_path.exists()

        reloaded = pd.read_parquet(parquet_path)
        assert len(reloaded) == 144

    def test_valid_days_csv_written(self, workspace):
        df = read_binance_csv(workspace["csv_path"])
        valid_df, invalid_df = compute_valid_invalid_days(df.index, 48)

        valid_path = workspace["processed_dir"] / "valid_days.csv"
        valid_df.to_csv(valid_path, index=False)
        assert valid_path.exists()

        loaded = pd.read_csv(valid_path)
        assert len(loaded) == 3
        assert "date_utc" in loaded.columns


# ---------------------------------------------------------------------------
# Stage 4: run_baseline  (spot engine, imported directly)
# ---------------------------------------------------------------------------

class TestStage4RunBaseline:
    @pytest.fixture
    def pipeline_outputs(self, workspace):
        """Run the full pipeline and return paths + results."""
        raw_dir = workspace["raw_dir"]
        processed_dir = workspace["processed_dir"]
        out_dir = workspace["reports_baseline_dir"]

        # --- Stage 1: hash ---
        manifest = build_manifest(raw_dir, ["*.csv"])
        manifest_path = workspace["root"] / "data" / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
        )

        # --- Stage 2: (quality checked via earlier tests) ---

        # --- Stage 3: build parquet + valid_days ---
        df, _ = load_dataset_from_manifest(raw_dir, manifest, "TESTUSDT", "30m")
        df = df.copy()
        df["date_utc"] = df.index.normalize().date.astype(str)

        parquet_path = processed_dir / "TESTUSDT_30m.parquet"
        df.to_parquet(parquet_path, index=True)

        valid_df, _ = compute_valid_invalid_days(df.index, 48)
        valid_days_path = processed_dir / "valid_days.csv"
        valid_df.to_csv(valid_days_path, index=False)

        # --- Stage 4: run baseline (spot) ---
        # Load OHLCV from parquet (same path run_baseline.py would take)
        df_raw = pd.read_parquet(parquet_path)

        # Load valid days
        from core.utils import load_valid_days_csv

        valid_days = load_valid_days_csv(valid_days_path)

        # Register ephemeral strategy plugin
        plugin_module = _register_strategy_plugin(df_raw)
        cfg = _build_config(plugin_module)

        # Build strategy signals
        from core.strategy_plugin import build_strategy_result

        strategy_result = build_strategy_result(
            df_raw=df_raw, cfg=cfg, valid_days=valid_days
        )

        # Run spot backtest
        from backtester.spot_engine import backtest_orb_strategy

        trades, equity_curve, final_capital, total_fees = backtest_orb_strategy(
            df=strategy_result.df_sig,
            orb_ranges=None,
            execution_specs=strategy_result.execution_specs,
            initial_capital=cfg["risk"]["initial_capital"],
            position_size=cfg["risk"]["position_size"],
            taker_fee_rate=cfg["fees"]["taker_fee_rate"],
            valid_days=valid_days,
        )

        # Write artifacts (same as run_baseline.py)
        trades_df = pd.DataFrame(trades)
        equity_df = pd.DataFrame(
            {"timestamp": strategy_result.df_sig.index, "equity": equity_curve}
        )
        trades_df.to_csv(out_dir / "trades.csv", index=False)
        equity_df.to_csv(out_dir / "equity_curve.csv", index=False)

        results = {
            "symbol": "TESTUSDT",
            "timeframe": "30m",
            "engine": "spot",
            "metrics": {
                "Initial Capital": float(cfg["risk"]["initial_capital"]),
                "Final Equity": float(
                    equity_df["equity"].iloc[-1] if not equity_df.empty else 10_000
                ),
                "Total Trades": len(trades),
                "Total Fees Paid": float(total_fees),
            },
        }
        results_path = out_dir / "results.json"
        results_path.write_text(
            json.dumps(results, indent=2, sort_keys=True), encoding="utf-8"
        )

        return {
            "trades": trades,
            "equity_curve": equity_curve,
            "final_capital": final_capital,
            "total_fees": total_fees,
            "trades_df": trades_df,
            "equity_df": equity_df,
            "results": results,
            "out_dir": out_dir,
            "valid_days": valid_days,
            "strategy_result": strategy_result,
        }

    def test_signals_generated(self, pipeline_outputs):
        """Strategy must produce at least one signal per valid day."""
        df_sig = pipeline_outputs["strategy_result"].df_sig
        signal_count = (df_sig["signal"] != 0).sum()
        assert signal_count == 3  # one per day

    def test_trades_produced(self, pipeline_outputs):
        """Backtest must produce at least one trade."""
        assert len(pipeline_outputs["trades"]) > 0

    def test_equity_curve_length(self, pipeline_outputs):
        """Equity curve must have one entry per bar."""
        assert len(pipeline_outputs["equity_curve"]) == 144

    def test_equity_never_negative(self, pipeline_outputs):
        """Equity must never go negative."""
        assert all(e >= 0 for e in pipeline_outputs["equity_curve"])

    def test_fees_charged(self, pipeline_outputs):
        """Fees must be non-zero when trades occur."""
        assert pipeline_outputs["total_fees"] > 0

    def test_trades_csv_written(self, pipeline_outputs):
        path = pipeline_outputs["out_dir"] / "trades.csv"
        assert path.exists()
        df = pd.read_csv(path)
        assert len(df) == len(pipeline_outputs["trades"])

    def test_equity_csv_written(self, pipeline_outputs):
        path = pipeline_outputs["out_dir"] / "equity_curve.csv"
        assert path.exists()
        df = pd.read_csv(path)
        assert len(df) == 144

    def test_results_json_written(self, pipeline_outputs):
        path = pipeline_outputs["out_dir"] / "results.json"
        assert path.exists()
        data = json.loads(path.read_text(encoding="utf-8"))
        assert data["symbol"] == "TESTUSDT"
        assert data["metrics"]["Total Trades"] == len(pipeline_outputs["trades"])

    def test_trade_fields_complete(self, pipeline_outputs):
        """Every trade must include the expected fields."""
        expected_fields = {
            "entry_time",
            "exit_time",
            "type",
            "signal_type",
            "entry_price",
            "exit_price",
            "target_price",
            "stop_loss",
            "position",
            "pnl",
            "exit_reason",
        }
        for trade in pipeline_outputs["trades"]:
            missing = expected_fields - set(trade.keys())
            assert not missing, f"Trade missing fields: {missing}"

    def test_all_trades_are_long(self, pipeline_outputs):
        """Strategy only generates long signals."""
        for trade in pipeline_outputs["trades"]:
            assert trade["type"] == "LONG"
            assert trade["signal_type"] == SIGNAL_TYPE


# ---------------------------------------------------------------------------
# Full pipeline smoke test (single function, no class)
# ---------------------------------------------------------------------------

def test_full_pipeline_smoke(tmp_path: Path):
    """Single smoke test exercising hash → quality → parquet → baseline."""
    # Setup
    raw_dir = tmp_path / "data" / "raw"
    raw_dir.mkdir(parents=True)
    processed_dir = tmp_path / "data" / "processed"
    processed_dir.mkdir(parents=True)

    csv_path = raw_dir / "TESTUSDT-30m-2024-01.csv"
    _write_synthetic_csv(csv_path, "2024-01-01", days=3)

    # 1) Hash
    manifest = build_manifest(raw_dir, ["*.csv"])
    assert manifest["file_count"] == 1

    # 2) Quality
    ts, nat = read_binance_timestamps_only(csv_path)
    assert nat == 0
    idx = pd.DatetimeIndex(ts).sort_values().drop_duplicates()
    gaps = summarize_gaps(idx, pd.Timedelta(minutes=30))
    assert len(gaps) == 0

    # 3) Parquet
    df, used = load_dataset_from_manifest(raw_dir, manifest, "TESTUSDT", "30m")
    assert len(df) == 144
    valid_df, invalid_df = compute_valid_invalid_days(df.index, 48)
    assert len(valid_df) == 3
    assert len(invalid_df) == 0

    df = df.copy()
    df["date_utc"] = df.index.normalize().date.astype(str)
    parquet_path = processed_dir / "TESTUSDT_30m.parquet"
    df.to_parquet(parquet_path, index=True)

    valid_days_path = processed_dir / "valid_days.csv"
    valid_df.to_csv(valid_days_path, index=False)

    # 4) Baseline
    df_raw = pd.read_parquet(parquet_path)
    from core.utils import load_valid_days_csv

    valid_days = load_valid_days_csv(valid_days_path)

    plugin_module = _register_strategy_plugin(df_raw)
    cfg = _build_config(plugin_module)

    from core.strategy_plugin import build_strategy_result

    result = build_strategy_result(df_raw=df_raw, cfg=cfg, valid_days=valid_days)

    from backtester.spot_engine import backtest_orb_strategy

    trades, equity, final_cap, fees = backtest_orb_strategy(
        df=result.df_sig,
        orb_ranges=None,
        execution_specs=result.execution_specs,
        initial_capital=10_000,
        position_size=0.95,
        taker_fee_rate=0.0005,
        valid_days=valid_days,
    )

    # Assertions across the full chain
    assert len(trades) > 0, "Pipeline must produce at least one trade"
    assert len(equity) == 144, "Equity curve must cover all bars"
    assert fees > 0, "Fees must be charged"
    assert final_cap > 0, "Capital must remain positive"
    assert all(e > 0 for e in equity), "Equity must stay positive"
