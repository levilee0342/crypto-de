import requests
import pandas as pd
from pathlib import Path

URL = "https://api.coingecko.com/api/v3/coins/markets"

params = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 50,
    "page": 1
}

def extract(run_date: str):
    response = requests.get(URL, params=params, timeout=50)
    response.raise_for_status() # Ném lỗi nếu có lỗi HTTP
    
    data = response.json()
    df = pd.DataFrame(data)

    ingestion_ts = pd.Timestamp.now(tz="UTC").floor("s")
    df["timestamp"] = ingestion_ts
    df["snapshot_date"] = run_date

    raw_dir = Path(f"data_lake/raw/dt={run_date}")
    raw_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(raw_dir / "crypto.parquet", index=False)

if __name__ == "__main__":
    extract("2026-03-17")