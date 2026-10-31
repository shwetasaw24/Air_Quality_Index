import csv
import subprocess
from pathlib import Path
import json

STATIONS_FILE = Path("metadata/final_stations.csv")
OUTPUT_FILE = Path("metadata/openaq_archive_inventory.csv")

START_YEAR = 2015
END_YEAR = 2025

BUCKET = "openaq-data-archive"


def get_s3_objects(prefix):
    """
    List OpenAQ archive objects under a prefix using AWS CLI.
    """
    command = [
        "aws",
        "s3api",
        "list-objects-v2",
        "--no-sign-request",
        "--bucket",
        BUCKET,
        "--prefix",
        prefix,
        "--output",
        "json"
    ]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print("AWS error:")
        print(result.stderr)
        return []

    if not result.stdout.strip():
        return []

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        print("Could not decode AWS response")
        return []

    return data.get("Contents", [])


def main():

    if not STATIONS_FILE.exists():
        raise FileNotFoundError(
            f"Missing {STATIONS_FILE}"
        )

    stations = []

    with open(STATIONS_FILE, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for row in reader:
            stations.append({
                "location_id": row["location_id"],
                "name": row.get("name", "")
            })

    print("=" * 70)
    print("OPENAQ ARCHIVE INVENTORY")
    print("=" * 70)

    print(f"Stations: {len(stations)}")
    print(f"Years: {START_YEAR}-{END_YEAR}")
    print()

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    total_files = 0
    total_bytes = 0

    with open(
        OUTPUT_FILE,
        "w",
        newline="",
        encoding="utf-8"
    ) as out:

        writer = csv.writer(out)

        writer.writerow([
            "location_id",
            "location_name",
            "year",
            "key",
            "size_bytes"
        ])

        for station_number, station in enumerate(stations, 1):

            location_id = station["location_id"]
            location_name = station["name"]

            print(
                f"[{station_number}/{len(stations)}] "
                f"{location_id} - {location_name}"
            )

            station_files = 0

            for year in range(START_YEAR, END_YEAR + 1):

                prefix = (
                    f"records/csv.gz/"
                    f"locationid={location_id}/"
                    f"year={year}/"
                )

                objects = get_s3_objects(prefix)

                for obj in objects:

                    key = obj["Key"]
                    size = obj["Size"]

                    writer.writerow([
                        location_id,
                        location_name,
                        year,
                        key,
                        size
                    ])

                    total_files += 1
                    total_bytes += size
                    station_files += 1

            print(
                f"    Files found: {station_files:,}"
            )

    print()
    print("=" * 70)
    print("INVENTORY COMPLETE")
    print("=" * 70)

    print(f"Total files: {total_files:,}")
    print(
        f"Compressed size: "
        f"{total_bytes / (1024**3):.2f} GB"
    )

    print(f"Saved: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()