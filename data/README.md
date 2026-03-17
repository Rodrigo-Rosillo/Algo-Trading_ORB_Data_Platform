# Data Directory

This repository does not ship raw or processed market data.

## Expected Structure

- `data/manifest.json`
- `data/raw/`
- `data/processed/`

## Typical Flow

1. Put your own exchange CSV files in `data/raw/`
2. Build a manifest:

```bash
python scripts/hash_data.py --data-dir data/raw --patterns *.csv --out data/manifest.json
```

3. Run data quality checks:

```bash
python scripts/data_quality.py --manifest data/manifest.json --out-dir reports/data_quality
```

4. Build processed parquet and valid-day lists:

```bash
python scripts/build_parquet.py --raw-manifest data/manifest.json --out-dir data/processed
```

The file names and symbols are fully user-defined. The repo only expects standard OHLCV-style CSV inputs supported by the ingestion scripts.
