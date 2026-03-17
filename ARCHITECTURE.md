# Architecture

This document explains how the ORB Data Platform is structured as a system.

The repository is intentionally split into a few clear responsibilities:

- data ingestion and curation
- strategy plug-in loading and validation
- backtest and walk-forward evaluation
- forward shadow and live/testnet execution
- state persistence, reporting, and operational monitoring

The project does not ship a repo-owned trading strategy. The platform owns the data, execution, safety, and reporting layers; the user supplies the signal-generation logic through a Python plug-in.

## System Overview

At a high level, the platform turns raw exchange CSVs into curated parquet data, then uses that data in one of three execution paths:

1. offline backtesting
2. walk-forward evaluation
3. forward execution in replay, live shadow, or live testnet mode

```text
raw CSV files
  -> scripts/hash_data.py
  -> data/manifest.json
  -> scripts/data_quality.py
  -> reports/data_quality/{quality.json, quality.html}
  -> scripts/build_parquet.py
  -> data/processed/{symbol_timeframe.parquet, valid_days.csv, invalid_days.csv, manifest.json}

curated market data
  -> core.strategy_plugin.build_strategy_result(...)
  -> user strategy plug-in
  -> validated signal frame + execution specs

validated signals
  -> scripts/run_baseline.py
  -> scripts/walk_forward.py
  -> scripts/forward_test.py

runtime outputs
  -> reports/baseline/*
  -> reports/walk_forward/*
  -> reports/forward_test/<run_id>/*
  -> ops/watchdog.py and ops/daily_report.py
```

## Design Principles

- Bring-your-own-strategy: signal logic lives in a user plug-in, not in the repository.
- Reproducibility: data inputs, configs, scripts, git state, and outputs are fingerprinted where practical.
- Separation of concerns: ingestion, signal generation, execution, persistence, and monitoring are distinct layers.
- Operational safety: risk controls, kill switches, reconciliation, heartbeat checks, and state recovery are built into the runtime path.
- Local-first artifacts: reports and generated datasets are produced locally and intentionally excluded from git.

## Repository Map

### `scripts/`

Primary entrypoints and workflow scripts.

- `scripts/hash_data.py`: walks raw files and builds `data/manifest.json` with file hashes and a combined dataset hash
- `scripts/data_quality.py`: audits timestamps and completeness, then writes JSON and HTML quality reports
- `scripts/build_parquet.py`: loads raw CSVs, builds curated parquet, computes valid and invalid days, and writes a processed manifest
- `scripts/run_baseline.py`: deterministic single-run backtest entrypoint
- `scripts/walk_forward.py`: fold-based evaluation over curated parquet data
- `scripts/forward_test.py`: orchestration entrypoint for replay shadow, live shadow, and live testnet flows
- `scripts/render_report.py`: renders an HTML summary from baseline outputs
- `scripts/forward_test_report.py`: builds a divergence report for a forward-test run
- `scripts/migrate_state_json_to_sqlite.py`: migrates legacy runtime state to the SQLite store
- `scripts/emergency_flatten.py`: operational helper to flatten exchange exposure

### `core/`

Shared platform logic.

- `core/strategy_plugin.py`: loads the configured strategy plug-in, validates the returned signal frame, and enforces the plug-in contract
- `core/utils.py`: shared helpers for hashing, JSON serialization, valid-day loading, and path/config support
- `core/orb.py`: ORB-related helper logic used by the execution layer

### `backtester/`

Offline execution engines and risk governance.

- `backtester/futures_engine.py`: batch futures backtest adapter
- `backtester/futures_core.py`: lower-level futures position and execution mechanics
- `backtester/spot_engine.py`: spot backtest engine
- `backtester/risk.py`: configurable risk limits, kill switches, and event recording

### `forward/`

Forward execution, streaming, artifacts, and runtime state.

- `forward/replay.py`: loads processed parquet for replay-based forward tests
- `forward/shadow.py`: deterministic shadow execution over historical data
- `forward/live_shadow.py`: live market-data ingestion with shadow execution only
- `forward/live_testnet.py`: live market-data ingestion with Binance Futures testnet order placement
- `forward/data_service.py`: websocket/data freshness supervision and stale-data kill switch handling
- `forward/trader_service.py`: runtime trade orchestration, reconciliation, persistence, and order lifecycle management
- `forward/state_store_sqlite.py`: SQLite WAL state store and migration utilities
- `forward/artifacts.py`: converts runtime state into CSV and JSONL artifacts
- `forward/testnet_broker.py`: exchange-specific broker adapter for testnet orders

### `ops/`

Operational monitoring and reporting.

- `ops/watchdog.py`: watches heartbeats, runtime health, and trade log activity
- `ops/daily_report.py`: summarizes daily trade performance and can send Telegram notifications

### `tests/`

Verification of plug-in behavior, engines, runtime recovery, and repository hygiene.

- `tests/unit/`: core unit and property-based tests
- `tests/integration/`: restart, replay, and state-recovery integration tests

## Data Layer

The data layer is built around three stages.

### 1. Raw Dataset Manifest

`scripts/hash_data.py` scans `data/raw/` and writes `data/manifest.json`.

The manifest records:

- relative file paths
- sizes and modification timestamps
- SHA-256 hashes for each file
- a stable combined dataset fingerprint

This gives the project a reproducible identifier for the exact raw dataset used in later steps.

### 2. Quality Audit

`scripts/data_quality.py` uses the manifest plus the configured symbol and timeframe to inspect raw bars.

It checks for:

- duplicate timestamps
- invalid timestamps
- missing bars across the expected frequency
- misaligned bars
- missing bars by UTC day

Outputs:

- `reports/data_quality/quality.json`
- `reports/data_quality/quality.html`

### 3. Curated Parquet Build

`scripts/build_parquet.py` loads the raw dataset defined by the manifest and writes a curated dataset into `data/processed/`.

Outputs:

- `<symbol>_<timeframe>.parquet`
- `valid_days.csv`
- `invalid_days.csv`
- `data/processed/manifest.json`

The processed manifest links the parquet output back to:

- the raw manifest hash
- the raw dataset hash
- the config hash
- the output hashes

This makes the curated layer traceable to its upstream inputs.

## Strategy Plug-In Layer

The strategy boundary is defined in `core/strategy_plugin.py`.

The configured plug-in is loaded from:

- `strategy_plugin.module`
- `strategy_plugin.callable`

The callable must return `core.strategy_plugin.StrategyBuildResult`, which contains:

- `df_sig`: a signal DataFrame aligned exactly to the input market-data index
- `execution_specs`: a mapping from `signal_type` to `ExecutionSpec`
- `strategy_metadata`: JSON-serializable metadata for reporting and audit outputs

The platform validates that:

- the signal frame index matches the market data exactly
- `signal` values are integer-like and finite
- non-zero signals have non-empty `signal_type`
- each `signal_type` has a matching execution spec
- signal direction matches execution side
- ORB-dependent execution specs have the required `orb_high` and `orb_low` fields

The starter implementation in `user_strategy.py` is a no-op plug-in used to validate the pipeline end to end.

## Execution Contract

Execution behavior is separated from signal generation through `execution_specs.py`.

`ExecutionSpec` defines:

- side: `long` or `short`
- target kind
- optional target percentage
- stop kind

This keeps the platform architecture modular:

- the plug-in decides when to emit a signal
- the execution layer decides how to translate that signal into target and stop behavior

## Evaluation Paths

### Baseline Backtest

`scripts/run_baseline.py` is the main offline evaluation entrypoint.

It:

- loads parquet if available, or falls back to raw CSVs via the manifest
- loads `valid_days.csv`
- builds signals through the configured plug-in
- runs either the spot or futures engine
- writes deterministic artifacts to `reports/baseline/`

Typical baseline artifacts include:

- `results.json`
- `trades.csv`
- `equity_curve.csv`
- `run_metadata.json`
- `hashes.json`
- optional `equity_curve.png`

### Walk-Forward Evaluation

`scripts/walk_forward.py` reuses the same plug-in and engine layers over multiple train/test windows.

It:

- loads curated parquet and valid days
- generates fold windows from configured train/test/step sizes
- evaluates only the test slice for each fold
- writes fold-level outputs and aggregate summaries

Outputs land under `reports/walk_forward/`.

## Forward Execution Paths

The forward runner is orchestrated by `scripts/forward_test.py`.

It supports three active paths:

### Replay + Shadow

Uses processed parquet as the data source and `forward/shadow.py` for deterministic shadow execution.

This path is useful for:

- validating the forward artifact model
- comparing replay shadow results against baseline expectations
- generating deterministic forward-test artifacts without exchange connectivity

### Live + Shadow

Uses live market data through websocket and REST bootstrap, but never places real or testnet orders.

`forward/live_shadow.py`:

- bootstraps recent history
- streams closed bars
- recomputes signals on the latest history
- runs a streaming shadow engine
- writes signals, orders, fills, positions, and events incrementally

### Live + Testnet

Uses live market data and a Binance Futures testnet broker adapter.

`forward/live_testnet.py` adds:

- broker initialization and leverage setup
- runtime state persistence to SQLite
- exchange-position reconciliation
- order polling and protection handling
- shutdown guards and optional smoke-test mode

This is the most operationally complete path in the repository.

## Runtime State And Recovery

The runtime state layer is implemented in `forward/state_store_sqlite.py`.

Key characteristics:

- SQLite with WAL mode
- integrity checks on open
- transactional updates to runner state and open position tables
- append-only trade log table
- JSON snapshot export for operator visibility
- migration path from legacy `state.json` to `state.db`

The SQLite schema stores:

- runner progress and bar counters
- daily halt state and reject counters
- current open position
- trade log events

This layer supports restart safety, reconciliation, and operator observability.

## Artifact Model

The project treats generated artifacts as part of the system design, not just side effects.

### Data Artifacts

- `data/manifest.json`
- `data/processed/*.parquet`
- `data/processed/valid_days.csv`
- `data/processed/invalid_days.csv`
- `data/processed/manifest.json`

### Baseline Artifacts

- `reports/baseline/results.json`
- `reports/baseline/trades.csv`
- `reports/baseline/equity_curve.csv`
- `reports/baseline/run_metadata.json`
- `reports/baseline/hashes.json`
- `reports/baseline/report.html`

### Forward-Test Artifacts

Each run writes to `reports/forward_test/<run_id>/`.

Common files:

- `signals.csv`
- `orders.csv`
- `fills.csv`
- `positions.csv`
- `events.jsonl`
- `run_metadata.json`
- `config_used.yaml`

Live/testnet runs may also produce:

- `state.db`
- `state.json`
- `shadow_stats.json`
- `forward_test_report.html`

These artifacts are intentionally local and excluded from git.

## Monitoring And Operations

The runtime layer is designed to support long-running deployments.

Monitoring features include:

- heartbeat file updates for Docker health checks
- websocket heartbeat and stale-data detection
- runtime event logging in `events.jsonl`
- trade-log-driven watchdog monitoring
- daily Telegram reporting when credentials are configured

Operational docs and deployment details live in `RUNBOOK.md`.

## Configuration Model

The primary configuration files are:

- `config.yaml`: baseline/default config
- `config_forward_test.yaml`: forward-run preset
- `.env`: runtime environment values, especially for deployed or testnet operation

Configuration is used to define:

- symbol and timeframe
- valid-day source
- strategy plug-in module and callable
- strategy-owned parameters under `strategy`
- fees, leverage, and risk controls
- forward-test mode, source, execution model, and runtime behavior

## Deployment Shape

The repository includes container support through:

- `Dockerfile`
- `docker-compose.yml`

The compose service persists runtime state to `/data`, exposes a heartbeat file for health checks, and is designed to run the forward-test runner inside a containerized environment.

## Testing Strategy

The test suite is intended to validate both correctness and repository hygiene.

Coverage areas include:

- strategy plug-in contract validation
- spot and futures execution behavior
- risk controls and kill switches
- forward reconciliation and shutdown behavior
- SQLite crash recovery and state migration
- replay and runtime integration behavior
- scrub checks that prevent committed generated artifacts or legacy identifiers from drifting back into the repo

## End-To-End Flow

For a typical workflow, the system operates in this order:

1. Put raw exchange CSV files in `data/raw/`
2. Build `data/manifest.json` with `scripts/hash_data.py`
3. Audit the raw dataset with `scripts/data_quality.py`
4. Build curated parquet and valid-day outputs with `scripts/build_parquet.py`
5. Point the config at a user strategy plug-in
6. Run offline evaluation through `scripts/run_baseline.py` or `scripts/walk_forward.py`
7. Run forward workflows through `scripts/forward_test.py`
8. Inspect outputs and operational status through reports, JSONL events, the SQLite state store, and ops helpers

## Boundaries And Non-Goals

This repository is intentionally not a research notebook dump or a strategy-optimization framework.

It does not aim to provide:

- repo-owned alpha logic
- strategy search or tuning workflows
- committed datasets or generated reports
- a warehouse or orchestration platform

Its role is narrower and cleaner:

- manage market-data ingestion and curation
- standardize the strategy plug-in contract
- execute backtest and forward workflows
- persist state safely
- emit reproducible artifacts
- support monitoring and operational recovery
