"""
Sugar Weather — One-Time Backfill
===================================
Fetches 2016-2026 + normals for Brazil (GO/MG/MS/SP), India, Thailand.
Saves one parquet file per origin in ./data/

Run once:
    python backfill.py

After this, use daily_update.py for incremental refreshes.
"""

import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import reduce
from pathlib import Path

# -------------------------------------------------------
# CONFIG
# -------------------------------------------------------
PARQUET_DIR = Path(__file__).parent / "data"
API_URL     = "https://api.weatherdesk.xweather.com/2e621a7f-2b1e-4f3e-af6a-5a986a68b398/services/gwi/v1/timeseries"
MAX_WORKERS = 20

FETCH_YEARS = [
    "2016", "2017", "2018", "2019", "2020",
    "2021", "2022", "2023", "2024", "2025", "2026",
    "normals",
]

# -------------------------------------------------------
# ORIGINS
# -------------------------------------------------------
ORIGINS = {
    "Brazil": {
        "file": "brazil.parquet",
        "stations": {
            # Goias (GO)
            "83423": "GO", "83470": "GO", "83526": "GO", "86712": "GO",
            "86713": "GO", "86731": "GO", "86732": "GO", "86734": "GO",
            "86737": "GO", "86751": "GO",
            # Minas Gerais (MG)
            "83531": "MG", "83538": "MG", "83566": "MG", "83574": "MG",
            "83579": "MG", "83582": "MG", "83592": "MG", "83687": "MG",
            "83692": "MG", "86740": "MG",
            # Mato Grosso do Sul (MS)
            "83552": "MS", "83565": "MS", "83612": "MS", "83702": "MS",
            "86772": "MS", "86791": "MS", "86792": "MS", "86808": "MS",
            "86810": "MS", "86813": "MS",
            # Sao Paulo (SP)
            "83630": "SP", "83716": "SP", "83726": "SP", "83780": "SP",
            "86814": "SP", "86815": "SP", "86816": "SP", "86817": "SP",
            "86818": "SP", "86822": "SP",
        },
    },
    "India": {
        "file": "india.parquet",
        "stations": {
            # Uttar Pradesh
            "42367": "Uttar Pradesh", "42369": "Uttar Pradesh",
            "42474": "Uttar Pradesh", "42479": "Uttar Pradesh",
            # Maharashtra
            "43063": "Maharashtra", "43157": "Maharashtra", "43158": "Maharashtra",
            "43113": "Maharashtra", "43117": "Maharashtra",
            # Karnataka
            "43200": "Karnataka", "43201": "Karnataka", "43263": "Karnataka",
            "43291": "Karnataka", "43295": "Karnataka", "43296": "Karnataka",
            "43197": "Karnataka", "43198": "Karnataka", "43160": "Karnataka",
            "43225": "Karnataka", "43226": "Karnataka", "43229": "Karnataka",
        },
    },
    "Thailand": {
        "file": "thailand.parquet",
        "stations": {
            "48431": "Thailand", "48381": "Thailand", "48437": "Thailand",
            "48432": "Thailand", "48354": "Thailand", "48390": "Thailand",
            "48400": "Thailand", "48378": "Thailand", "48379": "Thailand",
            "48440": "Thailand",
        },
    },
}

# -------------------------------------------------------
# FETCH
# -------------------------------------------------------
def _fetch_station(station: str, parameter: str) -> list:
    params = {
        "station": station, "parameter": parameter,
        "start": "01-01", "end": "12-31", "model": "0", "metric": "1",
    }
    r = requests.get(API_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json().get("output", {})

    records = []
    for api_year in FETCH_YEARS:
        if api_year not in data:
            continue
        label = "Normal (Maxar)" if api_year == "normals" else api_year
        for d in data[api_year]:
            rec = {"station": station, "year": label, "date": d["date"]}
            if parameter == "PRCP":
                rec["prcp"]     = d.get("prcp")
                rec["prcp_sum"] = d.get("prcp_sum")
            elif parameter == "TAVG":
                rec["tavg"] = d.get("tavg")
            elif parameter == "TMIN":
                rec["tmin"] = d.get("tmin")
            else:
                rec["tmax"] = d.get("tmax")
            records.append(rec)
    return records


def _fetch_origin(origin_name: str, cfg: dict) -> pd.DataFrame:
    station_region = cfg["stations"]
    stations       = list(station_region.keys())
    buckets        = {"PRCP": [], "TAVG": [], "TMIN": [], "TMAX": []}
    errors         = []

    tasks = [(s, p) for s in stations for p in buckets]
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(_fetch_station, s, p): (s, p) for s, p in tasks}
        for fut in as_completed(futures):
            stn, param = futures[fut]
            try:
                buckets[param].extend(fut.result())
            except Exception as e:
                errors.append(f"{stn}/{param}: {e}")

    if errors:
        print(f"  {len(errors)} error(s) (first 3): {errors[:3]}")

    frames = {p: pd.DataFrame(rows) for p, rows in buckets.items() if rows}
    if not frames:
        return pd.DataFrame()

    df = reduce(lambda l, r: l.merge(r, on=["station", "year", "date"], how="outer"),
                frames.values())
    for col in ["prcp", "prcp_sum", "tavg", "tmin", "tmax"]:
        if col not in df.columns:
            df[col] = pd.NA
    df["region"] = df["station"].map(station_region)
    return df[["station", "region", "year", "date", "prcp", "prcp_sum", "tavg", "tmin", "tmax"]]


# -------------------------------------------------------
# MAIN
# -------------------------------------------------------
def main():
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {PARQUET_DIR}\n")

    for origin_name, cfg in ORIGINS.items():
        n = len(cfg["stations"])
        print(f"[{origin_name}]  {n} stations x 4 params x {len(FETCH_YEARS)} years ...")
        df = _fetch_origin(origin_name, cfg)
        if df.empty:
            print(f"  No data returned -- skipping.\n")
            continue
        out = PARQUET_DIR / cfg["file"]
        df.to_parquet(out, index=False)
        print(f"  {len(df):,} rows saved -> {cfg['file']}")
        print(f"  Years: {sorted(df['year'].unique())}\n")

    print("Backfill complete.")


if __name__ == "__main__":
    main()
