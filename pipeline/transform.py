from pathlib import Path
import pandas as pd

def transform():
    raw_base = Path("data_lake/raw")
    raw_files = sorted(raw_base.glob("dt=*/crypto.parquet"))
    if not raw_files:
        raise FileNotFoundError("No raw file found")

    last_raw = raw_files[-1]
    run_date = last_raw.parent.name.split("=", 1)[1]

    df = pd.read_parquet(last_raw)
    df = df[[
        "id",
        "symbol",
        "current_price",
        "market_cap",
        "total_volume",
        "timestamp"
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
    transform()