"""
Sugar Weather — Daily Update
==============================
Refreshes current calendar year data in all parquet files.
Run daily via Task Scheduler (daily_push.bat).
"""

import requests
import pandas as pd
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import reduce
from pathlib import Path

PARQUET_DIR  = Path(__file__).parent / "data"
API_URL      = "https://api.weatherdesk.xweather.com/2e621a7f-2b1e-4f3e-af6a-5a986a68b398/services/gwi/v1/timeseries"
MAX_WORKERS  = 20
CURRENT_YEAR = str(datetime.date.today().year)

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


def _fetch_station(station: str, parameter: str) -> list:
    params = {
        "station": station, "parameter": parameter,
        "start": "01-01", "end": "12-31", "model": "0", "metric": "1",
    }
    r = requests.get(API_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json().get("output", {})

    records = []
    if CURRENT_YEAR in data:
        for d in data[CURRENT_YEAR]:
            rec = {"station": station, "year": CURRENT_YEAR, "date": d["date"]}
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


def _update_origin(origin_name: str, cfg: dict):
    parquet_path   = PARQUET_DIR / cfg["file"]
    station_region = cfg["stations"]
    stations       = list(station_region.keys())

    if not parquet_path.exists():
        print(f"  Parquet not found. Run backfill.py first.")
        return

    buckets = {"PRCP": [], "TAVG": [], "TMIN": [], "TMAX": []}
    errors  = []
    tasks   = [(s, p) for s in stations for p in buckets]
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(_fetch_station, s, p): (s, p) for s, p in tasks}
        for fut in as_completed(futures):
            stn, param = futures[fut]
            try:
                buckets[param].extend(fut.result())
            except Exception as e:
                errors.append(f"{stn}/{param}: {e}")

    if errors:
        print(f"  {len(errors)} error(s): {errors[:3]}")

    frames = {p: pd.DataFrame(rows) for p, rows in buckets.items() if rows}
    if not frames:
        print(f"  No data returned for {CURRENT_YEAR}.")
        return

    new_df = reduce(lambda l, r: l.merge(r, on=["station", "year", "date"], how="outer"),
                    frames.values())
    for col in ["prcp", "prcp_sum", "tavg", "tmin", "tmax"]:
        if col not in new_df.columns:
            new_df[col] = pd.NA

    new_df["region"] = new_df["station"].map(station_region)
    new_df = new_df[["station", "region", "year", "date", "prcp", "prcp_sum", "tavg", "tmin", "tmax"]]

    existing = pd.read_parquet(parquet_path)
    existing = existing[existing["year"] != CURRENT_YEAR]
    updated  = pd.concat([existing, new_df], ignore_index=True)
    updated.to_parquet(parquet_path, index=False)
    print(f"  {len(new_df):,} rows updated for {CURRENT_YEAR} -> {cfg['file']}")


def main():
    today = datetime.date.today()
    print(f"Daily update -- {today}  (refreshing year {CURRENT_YEAR})\n")
    for origin_name, cfg in ORIGINS.items():
        print(f"[{origin_name}]")
        _update_origin(origin_name, cfg)
    print("\nDone.")


if __name__ == "__main__":
    main()
