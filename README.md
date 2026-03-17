# ORB Data Platform

Opening Range Breakout platform for raw-data ingestion, data quality auditing, parquet curation, backtesting, forward shadow runs, and live/testnet execution.

The repository no longer ships a built-in trading strategy. Users provide their own Python strategy plug-in through `strategy_plugin.module` and `strategy_plugin.callable` in the config.

## Data Engineering Highlights

- Reproducible raw-data manifests with file-level SHA-256 hashes and a combined dataset fingerprint
- Data quality auditing for missing, duplicate, misaligned, and invalid UTC bars with JSON and HTML outputs
- Curated parquet builds with `valid_days.csv`, `invalid_days.csv`, and processed manifests that preserve input and config lineage
- Deterministic run artifacts that capture config, dataset, script, git, and output hashes for auditability
- SQLite WAL state persistence with integrity checks, JSON-to-SQLite migration, and crash-recovery coverage
- Dockerized runtime helpers including heartbeat health checks, watchdog monitoring, and Telegram reporting
- Unit, integration, and property-based tests covering reconciliation, recovery, risk controls, and repo hygiene

## What Stays In Scope

- Raw CSV hashing, dataset lineage, and data quality checks
- Processed parquet curation and valid-day detection
- Deterministic reports and reproducible run metadata
- Spot and futures backtest engines
- Forward shadow execution and live/testnet runners
- Risk controls, SQLite state persistence and recovery, watchdog monitoring, Telegram reporting, and ops tooling

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

- [user_strategy.py](user_strategy.py)

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

Render the baseline HTML report:

```bash
python scripts/render_report.py
```

Generate a forward-test divergence report:

```bash
python scripts/forward_test_report.py --run-id <RUN_ID>
```

## Generated Artifacts

Typical workflows emit auditable artifacts rather than only console output:

- `data/manifest.json` stores file-level hashes and a combined dataset fingerprint
- `reports/data_quality/quality.json` and `quality.html` summarize completeness and timestamp issues
- `data/processed/*.parquet`, `valid_days.csv`, `invalid_days.csv`, and `data/processed/manifest.json` record the curated dataset build
- `reports/baseline/` includes `results.json`, `trades.csv`, `equity_curve.csv`, `run_metadata.json`, `hashes.json`, and `report.html`
- `reports/forward_test/<run_id>/` includes `events.jsonl`, `orders.csv`, `fills.csv`, `positions.csv`, state snapshots, and `forward_test_report.html`

## Monitoring And Reporting

The platform still includes operational monitoring helpers for deployed runs:

- `ops/watchdog.py` monitors heartbeat freshness, container or process health, and trade log activity
- `ops/daily_report.py` builds a daily performance summary and can send Telegram updates when credentials are configured
- forward and live runners maintain heartbeat files used by Docker health checks and watchdog-style monitoring

See `RUNBOOK.md` for deployment and monitoring details.

## Data Layout

- [data/README.md](data/README.md)
- `data/raw/` is intentionally empty in git
- `data/processed/` is intentionally empty in git
- `reports/` is intentionally empty in git
- generated artifacts stay local, and repo scrub tests guard against committing them by mistake

## Tests

Run the full local suite:

```bash
pytest tests/unit -v
pytest tests/integration -v
```

The test suite covers the plug-in contract, execution engines, live/testnet services, SQLite crash recovery, reconciliation logic, property-based risk checks, and scrub regressions that guard against reintroducing legacy strategy identifiers or committed artifacts.

## Safety Note

This repository is for software development and research workflows. It is not financial advice. Validate your own strategy plug-in in shadow mode before connecting any real account.
