# Risk Controls

The platform ships generic risk controls for survivability and operational safety. They are independent of any particular strategy plug-in.

## Supported Controls

- Maximum position margin fraction
- Maximum leverage cap
- Maximum daily loss halt
- Maximum drawdown circuit breaker
- Maximum consecutive losses
- Maximum exposure duration
- Kill switches for stale data, repeated order rejects, and margin-ratio stress

## Where The Logic Lives

- [backtester/risk.py](/c:/Users/wrodr/Documents/Remote%20Work/CV%20proyecto/Algo-Trading_ORB_Data_Platform/backtester/risk.py)
- [backtester/futures_engine.py](/c:/Users/wrodr/Documents/Remote%20Work/CV%20proyecto/Algo-Trading_ORB_Data_Platform/backtester/futures_engine.py)
- [forward/trader_service.py](/c:/Users/wrodr/Documents/Remote%20Work/CV%20proyecto/Algo-Trading_ORB_Data_Platform/forward/trader_service.py)

## Configs

- `config.yaml` keeps a conservative default template
- `config_phase4.yaml` enables the stricter risk profile template

## Example Runs

```bash
python scripts/run_baseline.py --engine spot --config config_phase4.yaml
python scripts/run_baseline.py --engine futures --config config_phase4.yaml --leverage 2 --funding-per-8h 0.0001
```

Review the run outputs for `engine_stats.risk` when the futures engine is used.
