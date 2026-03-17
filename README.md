# ORB Data Platform

Generic Opening Range Breakout platform for data ingestion, data quality checks, backtesting, forward shadow runs, and live/testnet execution.

The repository no longer ships a built-in trading strategy. Users provide their own Python strategy plug-in through `strategy_plugin.module` and `strategy_plugin.callable` in the config.

## What Stays In Scope

- Raw CSV hashing and data quality checks
- Processed parquet generation and valid-day detection
- Spot and futures backtest engines
- Forward shadow execution and live/testnet runners
- Risk controls, state persistence, and ops tooling

## What Is Intentionally Not Included

- Any repo-owned signal logic
- Any tuning or strategy search workflow
- Any committed research outputs or processed snapshots
- Any dataset that reveals a prior production setup

## Quickstart

Create a virtual environment and install dependencies:

```bash
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

Review and update the config:

```bash
python scripts/data_quality.py --help
python scripts/build_parquet.py --help
python scripts/run_baseline.py --help
python scripts/walk_forward.py --help
python scripts/forward_test.py --help
```

### First Successful Run

Use this sequence to validate the platform end to end with your own raw files:

1. Place source CSV files in `data/raw/`.
2. Build the raw-data manifest and quality report:

```bash
python scripts/hash_data.py --data-dir data/raw --patterns *.csv --out data/manifest.json
python scripts/data_quality.py --manifest data/manifest.json --out-dir reports/data_quality
```

3. Build the processed parquet dataset:

```bash
python scripts/build_parquet.py --raw-manifest data/manifest.json --out-dir data/processed
```

4. Run the backtest entrypoint:

```bash
python scripts/run_baseline.py --engine futures --config config.yaml
```

If `config.yaml` still points to the starter `user_strategy.py`, the run will verify the full pipeline with a no-op strategy plug-in. Replace that plug-in when you are ready to test your own strategy logic.

## Strategy Plug-In Contract

Set the plug-in in `config.yaml`:

```yaml
strategy_plugin:
  module: user_strategy
  callable: build_strategy

strategy: {}
```

Your callable must return `core.strategy_plugin.StrategyBuildResult`.

Required output behavior:

- `df_sig` must use the same `DatetimeIndex` as the input market data
- `df_sig` must include `signal` and `signal_type`
- zero signals must use an empty `signal_type`
- every non-zero `signal_type` must have a matching `ExecutionSpec`
- signal sign and execution side must agree
- rows that need ORB bracket values must provide `orb_high` and/or `orb_low`

Starter file:

- [user_strategy.py](/c:/Users/wrodr/Documents/Remote%20Work/CV%20proyecto/Algo-Trading_ORB_Data_Platform/user_strategy.py)

## Main Commands

Build a raw-data manifest:

```bash
python scripts/hash_data.py --data-dir data/raw --patterns *.csv --out data/manifest.json
```

Run data quality checks:

```bash
python scripts/data_quality.py --manifest data/manifest.json --out-dir reports/data_quality
```

Build processed parquet and valid-day lists:

```bash
python scripts/build_parquet.py --raw-manifest data/manifest.json --out-dir data/processed
```

Run a backtest:

```bash
python scripts/run_baseline.py --engine futures --config config.yaml
```

Run walk-forward evaluation:

```bash
python scripts/walk_forward.py --engine futures --config config.yaml --data data/processed/BTCUSDT_30m.parquet
```

Run forward shadow or testnet mode:

```bash
python scripts/forward_test.py --config config_forward_test.yaml --mode shadow --source replay
python scripts/forward_test.py --config config_forward_test.yaml --mode testnet --source live
```

## Data Layout

- [data/README.md](/c:/Users/wrodr/Documents/Remote%20Work/CV%20proyecto/Algo-Trading_ORB_Data_Platform/data/README.md)
- `data/raw/` is intentionally empty in git
- `data/processed/` is intentionally empty in git
- `reports/` is intentionally empty in git

## Tests

Run the full local suite:

```bash
pytest tests/unit -v
pytest tests/integration -v
```

The test suite covers the plug-in contract, execution engines, live/testnet services, and a scrub regression check that guards against reintroducing legacy strategy identifiers or committed artifacts.

## Safety Note

This repository is for software development and research workflows. It is not financial advice. Validate your own strategy plug-in in shadow mode before connecting any real account.
