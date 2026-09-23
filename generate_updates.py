
"""
generate_updates.py
===================
Generates three synthetic incremental update files that simulate a "v2" data
release from the municipality. Run this script ONCE, before running the
incremental pipeline (week3_incremental.py).

Requirements: pandas, pyarrow, numpy
    pip install pandas pyarrow numpy

Usage (from the project root):
    python generate_updates.py
"""

import os
import math
import random
import numpy as np
import pandas as pd

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────
DATASETS_DIR   = "datasets"
OUTPUT_DIR     = os.path.join(DATASETS_DIR, "updates")
RANDOM_SEED    = 42

random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ══════════════════════════════════════════════════════════════════════════════
# 1.  TAXI TRIPS UPDATE
#     Source:  yellow_tripdata_2024-03.parquet   (~3 582 628 rows)
#     Target:  datasets/updates/yellow_tripdata_update.parquet
#     New rows: 5-10 % of source  -> ~179 131 - 358 263
#     Duplicates: 1-2 % of source -> ~35 826 - 71 653
# ══════════════════════════════════════════════════════════════════════════════
def generate_taxi_update():
    print("=" * 60)
    print("Generating taxi trips update ...")

    src_path = os.path.join(DATASETS_DIR, "yellow_tripdata_2024-03.parquet")
    df = pd.read_parquet(src_path)

    total_rows       = len(df)
    new_pct          = 0.07          # 7 % new trips
    dup_pct          = 0.015         # 1.5 % duplicates
    n_new            = math.ceil(total_rows * new_pct)
    n_dup            = math.ceil(total_rows * dup_pct)

    print(f"  Source rows          : {total_rows:,}")
    print(f"  New trips to generate: {n_new:,}  ({new_pct*100:.0f}%)")
    print(f"  Duplicate rows       : {n_dup:,}  ({dup_pct*100:.0f}%)")

    # ── Find the latest pickup timestamp in the original dataset ──────────────
    pickup_col  = "tpep_pickup_datetime"
    dropoff_col = "tpep_dropoff_datetime"
    df[pickup_col]  = pd.to_datetime(df[pickup_col])
    df[dropoff_col] = pd.to_datetime(df[dropoff_col])
    latest_ts = df[pickup_col].max()
    print(f"  Latest pickup in source: {latest_ts}")

    # ── Sample n_new rows from the original as a distribution template ────────
    # We sample WITH replacement so we always get enough rows even if n_new > len(df)
    sampled = df.sample(n=n_new, replace=True, random_state=RANDOM_SEED).copy()

    # ── Shift timestamps into April 2024 ──────────────────────────────────────
    # Each sampled row gets a random offset between 1 day and 31 days after the
    # latest trip in the original file (end of March 2024).
    offset_seconds = np.random.randint(
        low  = 1 * 24 * 3600,     # 1 day after end of March
        high = 31 * 24 * 3600,    # up to 31 days later (end of April)
        size = n_new,
    )
    offsets = pd.to_timedelta(offset_seconds, unit="s")

    # Compute trip duration from the original rows (keep realistic)
    trip_duration = sampled[dropoff_col] - sampled[pickup_col]

    # Build new timestamps
    new_pickup  = latest_ts + offsets
    new_dropoff = new_pickup + trip_duration

    sampled[pickup_col]  = new_pickup.values
    sampled[dropoff_col] = new_dropoff.values

    # Add small jitter to fare / distance so they are not exact copies
    jitter = lambda col, scale=0.05: sampled[col] * (1 + np.random.uniform(-scale, scale, size=n_new))
    sampled["trip_distance"] = jitter("trip_distance").round(2)
    sampled["fare_amount"]   = jitter("fare_amount").round(2)
    sampled["tip_amount"]    = jitter("tip_amount").round(2)
    def safe_get(col):
        if col in sampled.columns:
            return sampled[col].fillna(0)
        # Some parquet files have 'Airport_fee' instead of 'airport_fee'
        if col == "airport_fee" and "Airport_fee" in sampled.columns:
            return sampled["Airport_fee"].fillna(0)
        return 0

    sampled["total_amount"]  = (
        sampled["fare_amount"]
        + safe_get("extra")
        + safe_get("mta_tax")
        + sampled["tip_amount"]
        + safe_get("tolls_amount")
        + safe_get("improvement_surcharge")
        + safe_get("congestion_surcharge")
        + safe_get("airport_fee")
    ).round(2)

    # ── Create duplicate rows (exact copies from the original) ────────────────
    duplicates = df.sample(n=n_dup, replace=False, random_state=RANDOM_SEED + 1).copy()

    # ── Combine and shuffle ───────────────────────────────────────────────────
    update_df = pd.concat([sampled, duplicates], ignore_index=True)
    update_df = update_df.sample(frac=1, random_state=RANDOM_SEED + 2).reset_index(drop=True)

    # ── Save ──────────────────────────────────────────────────────────────────
    out_path = os.path.join(OUTPUT_DIR, "yellow_tripdata_update.parquet")
    update_df.to_parquet(out_path, index=False)

    print(f"  Total rows in update : {len(update_df):,}  ({n_new:,} new + {n_dup:,} duplicates)")
    print(f"  Saved -> {out_path}")
    print()

    return {
        "dataset"       : "taxi_trips",
        "new_records"   : n_new,
        "duplicates"    : n_dup,
        "total_rows"    : len(update_df),
        "schema_changes": "none (same schema as original)",
        "output_file"   : out_path,
    }


# ══════════════════════════════════════════════════════════════════════════════
# 2.  WEATHER UPDATE
#     Source:  datasets/weather.csv   (8 784 rows, Jan-Dec 2024)
#     Target:  datasets/updates/weather_update.csv
#     New rows: 10 days x 24 hours = 240 new hourly observations
#     Schema change: add column  humidity  (float, 20-100 %)
# ══════════════════════════════════════════════════════════════════════════════
def generate_weather_update():
    print("=" * 60)
    print("Generating weather update ...")

    src_path = os.path.join(DATASETS_DIR, "weather.csv")
    df = pd.read_csv(src_path)

    # Identify the last row's date/time
    # The weather dataset uses columns: year, month, day, hour
    df["_ts"] = pd.to_datetime(
        df["year"].astype(str) + "-"
        + df["month"].astype(str).str.zfill(2) + "-"
        + df["day"].astype(str).str.zfill(2) + " "
        + df["hour"].astype(str).str.zfill(2) + ":00:00"
    )
    last_ts = df["_ts"].max()
    print(f"  Last observation in source: {last_ts}")

    # Use the last ~168 rows (7 days) as a reference window for realistic values
    tail = df.sort_values("_ts").tail(168)

    # Pre-compute per-column stats from the reference window for variation
    numeric_cols = ["temp", "rhum", "prcp", "snwd", "wdir", "wspd", "wpgt", "pres", "cldc", "coco"]
    col_stats = {}
    for nc in numeric_cols:
        vals = pd.to_numeric(tail[nc], errors="coerce").dropna()
        col_stats[nc] = {
            "mean": float(vals.mean()) if len(vals) else 0.0,
            "std" : float(vals.std())  if len(vals) else 1.0,
        }

    # ── Generate 240 new hourly rows ──────────────────────────────────────────
    n_new   = 240    # 10 days x 24 hours
    rows    = []
    current = last_ts

    for i in range(n_new):
        current = current + pd.Timedelta(hours=1)

        def sample_val(col, lo=None, hi=None):
            s = col_stats[col]
            v = np.random.normal(s["mean"], s["std"] * 0.3)
            if lo is not None: v = max(lo, v)
            if hi is not None: v = min(hi, v)
            return round(v, 1)

        row = {
            "year" : current.year,
            "month": current.month,
            "day"  : current.day,
            "hour" : current.hour,
            "temp" : sample_val("temp"),
            "temp_source": "isd_lite",
            "rhum" : sample_val("rhum", lo=0, hi=100),
            "rhum_source": "isd_lite",
            "prcp" : max(0.0, sample_val("prcp", lo=0)),
            "prcp_source": "dwd_mosmix",
            "snwd" : None,
            "snwd_source": None,
            "wdir" : sample_val("wdir", lo=0, hi=360),
            "wdir_source": "isd_lite",
            "wspd" : sample_val("wspd", lo=0),
            "wspd_source": "isd_lite",
            "wpgt" : None,
            "wpgt_source": None,
            "pres" : sample_val("pres"),
            "pres_source": "isd_lite",
            "cldc" : sample_val("cldc", lo=0, hi=8),
            "cldc_source": "isd_lite",
            "coco" : round(np.clip(np.random.choice([1, 2, 3, 4, 5]), 1, 9)),
            "coco_source": "dwd_mosmix",
            # ── NEW COLUMN: humidity ──────────────────────────────────────────
            # Realistic relative humidity as a percentage (20-100).
            # Correlated with the rhum field but with added noise.
            "humidity": round(float(np.clip(
                np.random.normal(col_stats["rhum"]["mean"], col_stats["rhum"]["std"] * 0.4),
                20, 100
            )), 1),
        }
        rows.append(row)

    update_df = pd.DataFrame(rows)
    out_path  = os.path.join(OUTPUT_DIR, "weather_update.csv")
    update_df.to_csv(out_path, index=False)

    print(f"  New rows generated   : {n_new}")
    print(f"  New column added     : humidity  (float, 20-100 %)")
    print(f"  Date range           : {rows[0]['year']}-{rows[0]['month']:02d}-{rows[0]['day']:02d}  ->  "
          f"{rows[-1]['year']}-{rows[-1]['month']:02d}-{rows[-1]['day']:02d}")
    print(f"  Saved -> {out_path}")
    print()

    return {
        "dataset"       : "weather",
        "new_records"   : n_new,
        "duplicates"    : 0,
        "total_rows"    : n_new,
        "schema_changes": "added column: humidity (double, 20-100 %)",
        "output_file"   : out_path,
    }


# ══════════════════════════════════════════════════════════════════════════════
# 3.  AIR QUALITY UPDATE
#     Source:  datasets/air_quality.csv   (~8 139 551 rows)
#     Target:  datasets/updates/air_quality_update.csv
#     New rows: ~500-1 000 new hourly measurements
#     Schema change: add column  aqi  (int, 0-500)
#
#     NOTE: The file is 2.3 GB. We use chunked reading to get only the tail
#     rows we need -- we do not load the whole file into memory.
# ══════════════════════════════════════════════════════════════════════════════
def generate_air_quality_update():
    print("=" * 60)
    print("Generating air quality update ...")
    print("  (reading tail of 2.3 GB file -- this may take ~30-60 s)")

    src_path = os.path.join(DATASETS_DIR, "air_quality.csv")

    # Read in chunks; keep only the last chunk to get recent rows
    CHUNK_SIZE = 200_000
    last_chunk = None
    total_rows = 0
    for chunk in pd.read_csv(src_path, chunksize=CHUNK_SIZE, low_memory=False):
        last_chunk = chunk
        total_rows += len(chunk)

    print(f"  Total source rows (approx): {total_rows:,}")

    # Work with the last chunk as our template
    tail = last_chunk.copy()

    # Parse the latest date
    tail["date_local_parsed"] = pd.to_datetime(tail["Date Local"], errors="coerce")
    last_date = tail["date_local_parsed"].max()
    print(f"  Last date in source: {last_date.date()}")

    # Pick a representative set of unique monitoring stations from the tail
    station_cols = [
        "State Code", "County Code", "Site Num", "Parameter Code", "POC",
        "Latitude", "Longitude", "Datum", "Parameter Name", "Units of Measure",
        "MDL", "Method Type", "Method Code", "Method Name",
        "State Name", "County Name",
    ]
    stations = tail[station_cols].drop_duplicates().head(20)  # keep at most 20 stations
    n_stations = len(stations)
    print(f"  Unique monitoring stations to use: {n_stations}")

    # We generate 3 days of hourly data for each station
    n_days = 3
    rows   = []

    for day_offset in range(1, n_days + 1):
        obs_date = last_date + pd.Timedelta(days=day_offset)
        for hour in range(24):
            time_local_str = f"{hour:02d}:00"
            # GMT is UTC+5 for Eastern US (approximate)
            gmt_hour = (hour + 5) % 24
            gmt_date = obs_date + pd.Timedelta(hours=(hour + 5) // 24)
            time_gmt_str = f"{gmt_hour:02d}:00"

            for _, station in stations.iterrows():
                # Realistic PM2.5 sample measurements (microg/m3): 0-50
                sample_measurement = round(
                    max(0.0, np.random.lognormal(mean=2.0, sigma=0.6)), 1
                )

                # ── NEW COLUMN: aqi ───────────────────────────────────────────
                # AQI for PM2.5 is derived from the sample measurement.
                # Linear approximation: AQI ~= sample_measurement * 4 (rough).
                # Clipped to [0, 500].
                aqi_value = int(np.clip(round(sample_measurement * 4), 0, 500))

                row = {
                    "State Code"        : station["State Code"],
                    "County Code"       : station["County Code"],
                    "Site Num"          : station["Site Num"],
                    "Parameter Code"    : station["Parameter Code"],
                    "POC"               : int(station["POC"]) if pd.notna(station["POC"]) else 1,
                    "Latitude"          : station["Latitude"],
                    "Longitude"         : station["Longitude"],
                    "Datum"             : station["Datum"],
                    "Parameter Name"    : station["Parameter Name"],
                    "Date Local"        : obs_date.strftime("%Y-%m-%d"),
                    "Time Local"        : time_local_str,
                    "Date GMT"          : gmt_date.strftime("%Y-%m-%d"),
                    "Time GMT"          : time_gmt_str,
                    "Sample Measurement": sample_measurement,
                    "Units of Measure"  : station["Units of Measure"],
                    "MDL"               : station["MDL"],
                    "Uncertainty"       : None,
                    "Qualifier"         : None,
                    "Method Type"       : station["Method Type"],
                    "Method Code"       : station["Method Code"],
                    "Method Name"       : station["Method Name"],
                    "State Name"        : station["State Name"],
                    "County Name"       : station["County Name"],
                    "Date of Last Change": obs_date.strftime("%Y-%m-%d"),
                    # ── New column ────────────────────────────────────────────
                    "aqi"               : aqi_value,
                }
                rows.append(row)

    update_df = pd.DataFrame(rows)
    n_new     = len(update_df)
    out_path  = os.path.join(OUTPUT_DIR, "air_quality_update.csv")
    update_df.to_csv(out_path, index=False)

    print(f"  New rows generated   : {n_new:,}")
    print(f"  New column added     : aqi  (int, 0-500)")
    print(f"  Date range           : {(last_date + pd.Timedelta(days=1)).date()}  ->  "
          f"{(last_date + pd.Timedelta(days=n_days)).date()}")
    print(f"  Saved -> {out_path}")
    print()

    return {
        "dataset"       : "air_quality",
        "new_records"   : n_new,
        "duplicates"    : 0,
        "total_rows"    : n_new,
        "schema_changes": "added column: aqi (integer, 0-500)",
        "output_file"   : out_path,
    }


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    summaries = []

    summaries.append(generate_taxi_update())
    summaries.append(generate_weather_update())
    summaries.append(generate_air_quality_update())

    # ── Print documentation summary ───────────────────────────────────────────
    print("=" * 60)
    print("DOCUMENTATION SUMMARY")
    print("=" * 60)
    print()
    for s in summaries:
        print(f"Dataset      : {s['dataset']}")
        print(f"  New records  : {s['new_records']:,}")
        print(f"  Duplicates   : {s['duplicates']:,}")
        print(f"  Total rows   : {s['total_rows']:,}")
        print(f"  Schema change: {s['schema_changes']}")
        print(f"  Output file  : {s['output_file']}")
        print()
