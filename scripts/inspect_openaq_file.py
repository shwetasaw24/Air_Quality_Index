import gzip
import csv
import os
from collections import Counter


FILE_PATH = (
    "data/raw/openaq/location_5645/"
    "location-5645-20180311.csv.gz"
)


print("=" * 60)
print("OPENAQ FILE INSPECTION")
print("=" * 60)


if not os.path.exists(FILE_PATH):
    raise FileNotFoundError(FILE_PATH)


with gzip.open(
    FILE_PATH,
    "rt",
    encoding="utf-8"
) as f:

    reader = csv.DictReader(f)

    rows = list(reader)


print("\nFile:")
print(FILE_PATH)

print("\nNumber of rows:")
print(len(rows))

print("\nColumns:")
print(reader.fieldnames)


print("\nParameters:")

parameter_counts = Counter(
    row["parameter"]
    for row in rows
)

for parameter, count in parameter_counts.items():

    print(
        f"  {parameter:20} {count}"
    )


print("\nUnits:")

unit_counts = Counter(
    row["units"]
    for row in rows
)

for unit, count in unit_counts.items():

    print(
        f"  {unit:20} {count}"
    )


print("\nSensors:")

sensor_counts = Counter(
    row["sensors_id"]
    for row in rows
)

print(
    "Unique sensors:",
    len(sensor_counts)
)


print("\nSample records:")

for row in rows[:5]:

    print(row)


print("\nMissing values:")

for column in reader.fieldnames:

    missing = sum(
        1
        for row in rows
        if not row[column]
    )

    print(
        f"  {column:20} {missing}"
    )


print("\n" + "=" * 60)
print("INSPECTION COMPLETE")
print("=" * 60)