import pandas as pd
from pathlib import Path

INPUT = Path("metadata/station_coverage.csv")
OUTPUT = Path("metadata/final_stations.csv")

TARGET_STATIONS = 75

df = pd.read_csv(INPUT)

print("Columns found:")
print(df.columns.tolist())
print()

print(f"Total locations available: {len(df)}")

# Try to identify useful columns automatically
id_col = next(
    (c for c in df.columns if c.lower() in ["location_id", "locationid", "id"]),
    None
)

name_col = next(
    (c for c in df.columns if c.lower() in ["location", "name", "location_name"]),
    None
)

first_col = "first_measurement_utc"
last_col = "last_measurement_utc"

if id_col is None:
    raise ValueError(
        "Could not identify the location ID column. "
        "Run the column-printing command and inspect station_coverage.csv."
    )

print(f"Using ID column: {id_col}")
print(f"Using name column: {name_col}")
print(f"Using first-date column: {first_col}")
print(f"Using last-date column: {last_col}")

# Make a copy
selected = df.copy()

# Convert dates if available
if first_col:
    selected[first_col] = pd.to_datetime(
        selected[first_col], errors="coerce"
    )

if last_col:
    selected[last_col] = pd.to_datetime(
        selected[last_col], errors="coerce"
    )

# Prefer stations with long historical coverage
if first_col and last_col:
    selected["coverage_days"] = (
        selected[last_col] - selected[first_col]
    ).dt.days

    selected = selected.sort_values(
        by="coverage_days",
        ascending=False
    )

# Select the top stations
selected = selected.head(TARGET_STATIONS)

# Keep original columns plus calculated coverage
selected.to_csv(OUTPUT, index=False)

print()
print("=" * 60)
print(f"SELECTED {len(selected)} STATIONS")
print("=" * 60)

print(selected[[c for c in [id_col, name_col, first_col, last_col] if c]].to_string(index=False))

print()
print(f"Saved to: {OUTPUT}")