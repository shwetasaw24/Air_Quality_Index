import subprocess
import json
import csv
import os
import time


# ============================================================
# CONFIGURATION
# ============================================================

STATIONS_FILE = "station_coverage.csv"
OUTPUT_FILE = "archive_volume_all.csv"

START_YEAR = 2018
END_YEAR = 2025

# Process all Indian stations
MAX_STATIONS = None

# Number of stations after which progress is saved
SAVE_EVERY = 5


# ============================================================
# S3 INVENTORY
# ============================================================

def get_s3_objects(location_id):

    prefix = (
        f"records/csv.gz/"
        f"locationid={location_id}/"
    )

    command = [
        "aws",
        "s3api",
        "list-objects-v2",
        "--no-sign-request",
        "--bucket",
        "openaq-data-archive",
        "--prefix",
        prefix,
        "--output",
        "json"
    ]

    try:

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=300
        )

        if result.returncode != 0:

            print(
                f"\nS3 ERROR for station {location_id}"
            )

            print(result.stderr)

            return None

        data = json.loads(result.stdout)

        return data.get("Contents", [])

    except subprocess.TimeoutExpired:

        print(
            f"\nTIMEOUT for station {location_id}"
        )

        return None

    except Exception as e:

        print(
            f"\nERROR for station {location_id}: {e}"
        )

        return None


# ============================================================
# LOAD STATIONS
# ============================================================

def load_stations():

    if not os.path.exists(STATIONS_FILE):

        raise FileNotFoundError(
            f"Could not find {STATIONS_FILE}"
        )

    with open(
        STATIONS_FILE,
        "r",
        encoding="utf-8"
    ) as f:

        stations = list(
            csv.DictReader(f)
        )

    if MAX_STATIONS is not None:

        stations = stations[
            :MAX_STATIONS
        ]

    return stations


# ============================================================
# LOAD EXISTING RESULTS
# ============================================================

def load_existing_results():

    if not os.path.exists(OUTPUT_FILE):

        return {}

    results = {}

    with open(
        OUTPUT_FILE,
        "r",
        encoding="utf-8"
    ) as f:

        reader = csv.DictReader(f)

        for row in reader:

            results[
                row["location_id"]
            ] = row

    return results


# ============================================================
# SAVE RESULTS
# ============================================================

def save_results(results):

    if not results:

        return

    rows = list(
        results.values()
    )

    rows.sort(
        key=lambda x:
        int(x["location_id"])
    )

    fieldnames = list(
        rows[0].keys()
    )

    with open(
        OUTPUT_FILE,
        "w",
        newline="",
        encoding="utf-8"
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames
        )

        writer.writeheader()

        writer.writerows(rows)


# ============================================================
# ANALYZE ONE STATION
# ============================================================

def analyze_station(station):

    location_id = station[
        "location_id"
    ]

    name = station[
        "name"
    ]

    print()
    print(
        f"Station {location_id}: {name}"
    )

    objects = get_s3_objects(
        location_id
    )

    if objects is None:

        return None


    # Only retain archive measurement files
    objects = [

        obj for obj in objects

        if obj.get(
            "Key",
            ""
        ).endswith(
            ".csv.gz"
        )
    ]


    yearly_files = {

        year: 0

        for year in range(
            START_YEAR,
            END_YEAR + 1
        )
    }


    yearly_bytes = {

        year: 0

        for year in range(
            START_YEAR,
            END_YEAR + 1
        )
    }


    total_files = 0
    total_bytes = 0


    for obj in objects:

        key = obj.get(
            "Key",
            ""
        )

        size = obj.get(
            "Size",
            0
        )


        # Determine year from:
        #
        # locationid=X/year=YYYY/
        #

        match = None

        parts = key.split("/")

        for part in parts:

            if part.startswith(
                "year="
            ):

                match = part[
                    len("year="):
                ]

                break


        if match is None:

            continue


        try:

            year = int(match)

        except ValueError:

            continue


        if not (
            START_YEAR
            <= year
            <= END_YEAR
        ):

            continue


        yearly_files[
            year
        ] += 1


        yearly_bytes[
            year
        ] += size


        total_files += 1

        total_bytes += size


    active_years = sum(

        1

        for count in yearly_files.values()

        if count > 0

    )


    coverage_ratio = (
        active_years
        /
        (END_YEAR - START_YEAR + 1)
    )


    result = {

        "location_id":
            location_id,

        "station":
            name,

        "first_measurement":
            station.get(
                "first_measurement_utc",
                ""
            ),

        "last_measurement":
            station.get(
                "last_measurement_utc",
                ""
            ),

        "target_parameters":
            station.get(
                "target_parameters",
                ""
            ),

        "total_daily_files":
            total_files,

        "total_size_bytes":
            total_bytes,

        "total_size_mb":
            round(
                total_bytes
                /
                (1024 * 1024),
                2
            ),

        "total_size_gb":
            round(
                total_bytes
                /
                (1024 * 1024 * 1024),
                4
            ),

        "active_years":
            active_years,

        "coverage_ratio":
            round(
                coverage_ratio,
                3
            )
    }


    for year in range(
        START_YEAR,
        END_YEAR + 1
    ):

        result[
            f"files_{year}"
        ] = yearly_files[year]

        result[
            f"size_mb_{year}"
        ] = round(
            yearly_bytes[year]
            /
            (1024 * 1024),
            2
        )


    print(
        f"  Daily files : {total_files}"
    )

    print(
        f"  Size        : "
        f"{result['total_size_mb']} MB"
    )

    print(
        f"  Active years: "
        f"{active_years}/{END_YEAR - START_YEAR + 1}"
    )


    return result


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)

    print(
        "OPENAQ ARCHIVE INVENTORY"
    )

    print("=" * 70)

    print(
        f"Years: {START_YEAR}–{END_YEAR}"
    )

    print(
        "Scope: All Indian stations"
    )

    print()


    stations = load_stations()

    existing = load_existing_results()

    print(
        f"Stations found: {len(stations)}"
    )

    print(
        f"Already completed: "
        f"{len(existing)}"
    )


    results = existing.copy()


    for index, station in enumerate(
        stations,
        start=1
    ):

        location_id = station[
            "location_id"
        ]


        if location_id in results:

            print(
                f"[{index}/{len(stations)}] "
                f"{location_id} - SKIPPED"
            )

            continue


        print(
            f"\n[{index}/{len(stations)}]"
        )


        result = analyze_station(
            station
        )


        if result is not None:

            results[
                location_id
            ] = result


        # Save periodically
        if (
            index % SAVE_EVERY == 0
            or index == len(stations)
        ):

            save_results(
                results
            )

            print(
                f"\nProgress saved: "
                f"{len(results)} stations"
            )


        # Small pause to be polite
        time.sleep(0.2)


    save_results(
        results
    )


    # ========================================================
    # FINAL RANKING
    # ========================================================

    completed = list(
        results.values()
    )


    completed.sort(

        key=lambda x:
        int(
            x["total_size_bytes"]
        ),

        reverse=True
    )


    print()
    print("=" * 70)
    print(
        "ARCHIVE INVENTORY COMPLETED"
    )
    print("=" * 70)

    print(
        f"Stations analyzed: "
        f"{len(completed)}/{len(stations)}"
    )

    print()
    print(
        "TOP 30 BY ARCHIVE SIZE"
    )

    print("-" * 70)


    for rank, row in enumerate(
        completed[:30],
        start=1
    ):

        print(
            f"{rank:2}. "
            f"{row['location_id']:>6} | "
            f"{row['station'][:42]:42} | "
            f"{row['total_daily_files']:5} files | "
            f"{row['total_size_mb']:8.2f} MB | "
            f"{row['active_years']} years"
        )


    print()
    print(
        f"Output: {OUTPUT_FILE}"
    )


if __name__ == "__main__":

    main()