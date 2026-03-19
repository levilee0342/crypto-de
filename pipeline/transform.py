from pathlib import Path
import pandas as pd

def transform(run_date: str):
    raw_file = Path(f"data_lake/raw/dt={run_date}/crypto.parquet")
    if not raw_file.exists():
        raise FileNotFoundError(f"Raw file not found for run_date={run_date}")

    df = pd.read_parquet(raw_file)
    df = df[[
        "id",
        "symbol",
        "current_price",
        "market_cap",
        "total_volume",
        "timestamp",
        "snapshot_date",
    ]].copy()

    df = df.rename(columns={
        "id":"coin_id",
        "current_price":"price",
        "total_volume":"volume"
    })

    processed_dir = Path(f"data_lake/processed/dt={run_date}")
    processed_dir.mkdir(parents=True, exist_ok=True)

    df.to_parquet(processed_dir / "crypto_clean.parquet", index=False)

if __name__ == "__main__":
    transform("2026-03-17")