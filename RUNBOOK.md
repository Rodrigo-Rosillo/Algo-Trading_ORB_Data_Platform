# RUNBOOK

Operational guide for running the generic ORB platform in shadow or testnet/live environments.

## Quick Facts

- Exchange connectivity: Binance spot or USD-M futures, depending on your config
- Strategy source: user-supplied plug-in configured in `strategy_plugin`
- State storage: SQLite WAL under the configured run directory or `STATE_DB_PATH`
- Heartbeat file: `HEARTBEAT_PATH` or the runner default
- Service name: `trader` if you deploy with Docker Compose

## Normal Operations

From the repo root:

```bash
docker compose up -d --build
docker compose ps
docker compose logs -f trader
```

Stop or restart:

```bash
docker compose stop trader
docker compose restart trader
```

Deploy a code change:

```bash
git pull
docker compose up -d --build
docker compose ps
```

## Environment

- Keep `.env` out of git
- Restrict `.env` permissions
- Store only the credentials needed for the chosen environment

```bash
chmod 600 .env
```

## Emergency Procedures

Stop the service:

```bash
docker compose stop trader
```

Flatten open positions with the standalone helper:

```bash
BINANCE_TESTNET_API_KEY=... BINANCE_TESTNET_API_SECRET=... python scripts/emergency_flatten.py --testnet
BINANCE_API_KEY=... BINANCE_API_SECRET=... python scripts/emergency_flatten.py
```

Or through Docker:

```bash
BINANCE_TESTNET_API_KEY=... BINANCE_TESTNET_API_SECRET=... docker compose run --rm trader python scripts/emergency_flatten.py --testnet
BINANCE_API_KEY=... BINANCE_API_SECRET=... docker compose run --rm trader python scripts/emergency_flatten.py
```

## State Recovery

Check SQLite integrity:

```bash
sqlite3 /path/to/state.db "PRAGMA integrity_check;"
```

If the result is not `ok`, stop the service, move the database aside, restore the latest backup, and restart.

## Backups

Create local backups:

```bash
mkdir -p backups
cp /path/to/state.db backups/state.db.$(date -u +%Y%m%dT%H%M%SZ)
```

Restore:

```bash
docker compose stop trader
cp /path/to/backup/state.db /path/to/state.db
docker compose up -d
```

## Monitoring

- Watch the heartbeat file freshness
- Watch the service logs
- Review emitted events for kill-switch or reconciliation failures
- Verify that the configured strategy plug-in and symbol match the intended deployment before enabling live order placement
