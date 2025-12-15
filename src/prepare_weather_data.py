from pathlib import Path
import xarray as xr
import pandas as pd

IN_DIR = Path("./data")
OUT_DIR = Path("./data/weather")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def month_key_from_time(t: pd.Timestamp) -> str:
    return f"{t.year:04d}-{t.month:02d}"

buffer = []
current_key = None

nc_files = sorted(IN_DIR.glob("*.nc"))

for nc_path in nc_files:
    ds = xr.open_dataset(nc_path)
    df = ds.to_dataframe().reset_index()
    ds.close()

    df["time"] = pd.to_datetime(df["time"])
    key = month_key_from_time(df["time"].iloc[0])

    if current_key is None:
        current_key = key

    if key != current_key:
        out_file = OUT_DIR / f"weather_{current_key}.parquet"
        pd.concat(buffer, ignore_index=True).to_parquet(out_file, engine="pyarrow", index=False)
        buffer = []
        current_key = key

    buffer.append(df)

if buffer:
    out_file = OUT_DIR / f"weather_{current_key}.parquet"
    pd.concat(buffer, ignore_index=True).to_parquet(out_file, engine="pyarrow", index=False)
