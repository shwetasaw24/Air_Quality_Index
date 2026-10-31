import csv
import gzip
import os
import random
import subprocess
import json
from collections import Counter


# ============================================================
# CONFIGURATION
# ============================================================

INVENTORY_FILE = "archive_volume_all.csv"

SAMPLE_DIR = "data/raw/openaq/sample"

SAMPLE_FILES_PER_YEAR = 5

YEARS_TO_SAMPLE = [
    2018,
    2019,
    2020,
    2021,
    2022,
    2025
]


# ============================================================
# HELPERS
# ============================================================

def load_inventory():

    with open(
        INVENTORY_FILE,
        "r",
        encoding="utf-8"
    ) as f:

        return list(
            csv.DictReader(f)
        )


def get_objects(location_id):

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

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=300
    )

    if result.returncode != 0:

        print(
            "S3 error:",
            result.stderr
        )

        return []

    data = json.loads(
        result.stdout
    )

    return data.get(
        "Contents",
        []
    )


def extract_year(key):

    for part in key.split("/"):

        if part.startswith("year="):

            try:
                return int(
                    part.replace(
                        "year=",
                        ""
                    )
                )

            except ValueError:
                return None

    return None


def choose_sample_files(
    objects,
    year,
    number
):

    files = []

    for obj in objects:

        key = obj.get(
            "Key",
            ""
        )

        if not key.endswith(
            ".csv.gz"
        ):
            continue

        if extract_year(key) != year:
            continue

        files.append(obj)


    if not files:
        return []


    if len(files) <= number:

        return files


    return random.sample(
        files,
        number
    )


def download_file(
    s3_key,
    destination
):

    os.makedirs(
        os.path.dirname(
            destination
        ),
        exist_ok=True
    )

    command = [
        "aws",
        "s3",
        "cp",
        "--no-sign-request",
        f"s3://openaq-data-archive/{s3_key}",
        destination
    ]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True
    )

    if result.returncode != 0:

        print(
            "Download failed:",
            result.stderr
        )

        return False

    return True


# ============================================================
# INSPECT CSV.GZ
# ============================================================

def inspect_file(path):

    rows = 0

    parameters = Counter()

    sensors = Counter()

    units = Counter()

    missing_values = Counter()

    first_datetime = None
    last_datetime = None


    with gzip.open(
        path,
        "rt",
        encoding="utf-8"
    ) as f:

        reader = csv.DictReader(f)

        for row in reader:

            rows += 1

            parameters[
                row["parameter"]
            ] += 1

            sensors[
                row["sensors_id"]
            ] += 1

            units[
                row["units"]
            ] += 1


            for column in reader.fieldnames:

                if not row.get(
                    column,
                    ""
                ).strip():

                    missing_values[
                        column
                    ] += 1


            dt = row.get(
                "datetime"
            )

            if dt:

                if (
                    first_datetime is None
                    or dt < first_datetime
                ):

                    first_datetime = dt

                if (
                    last_datetime is None
                    or dt > last_datetime
                ):

                    last_datetime = dt


    compressed_size = os.path.getsize(
        path
    )


    return {

        "rows": rows,

        "compressed_bytes":
            compressed_size,

        "parameters":
            parameters,

        "sensors":
            sensors,

        "units":
            units,

        "missing":
            missing_values,

        "first_datetime":
            first_datetime,

        "last_datetime":
            last_datetime
    }


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)

    print(
        "OPENAQ HISTORICAL DATA SAMPLING"
    )

    print("=" * 70)


    os.makedirs(
        SAMPLE_DIR,
        exist_ok=True
    )


    stations = load_inventory()


    # Choose stations with useful archive volume
    stations = sorted(
        stations,
        key=lambda x:
            int(
                x["total_size_bytes"]
            ),
        reverse=True
    )


    # Select 10 geographically diverse-ish
    # high-volume candidates.
    selected_stations = stations[:10]


    print(
        "\nSelected stations:"
    )

    for station in selected_stations:

        print(
            station["location_id"],
            "|",
            station["station"]
        )


    all_samples = []


    for station in selected_stations:

        location_id = station[
            "location_id"
        ]

        print()
        print(
            "=" * 70
        )

        print(
            "Station:",
            location_id,
            station["station"]
        )

        print(
            "=" * 70
        )


        objects = get_objects(
            location_id
        )


        for year in YEARS_TO_SAMPLE:

            samples = choose_sample_files(
                objects,
                year,
                SAMPLE_FILES_PER_YEAR
            )


            print(
                f"{year}: "
                f"{len(samples)} files"
            )


            for obj in samples:

                key = obj["Key"]

                filename = os.path.basename(
                    key
                )

                destination = os.path.join(
                    SAMPLE_DIR,
                    filename
                )


                print(
                    "  Downloading:",
                    filename
                )


                if not download_file(
                    key,
                    destination
                ):

                    continue


                info = inspect_file(
                    destination
                )


                all_samples.append({

                    "location_id":
                        location_id,

                    "station":
                        station["station"],

                    "year":
                        year,

                    "file":
                        filename,

                    "rows":
                        info["rows"],

                    "compressed_bytes":
                        info["compressed_bytes"],

                    "first_datetime":
                        info["first_datetime"],

                    "last_datetime":
                        info["last_datetime"],

                    "parameters":
                        ",".join(
                            info["parameters"].keys()
                        ),

                    "parameter_counts":
                        str(
                            dict(
                                info["parameters"]
                            )
                        ),

                    "units":
                        ",".join(
                            info["units"].keys()
                        ),

                    "sensor_count":
                        len(
                            info["sensors"]
                        ),

                    "missing_total":
                        sum(
                            info["missing"].values()
                        )
                })


    # ========================================================
    # SAVE SAMPLE RESULTS
    # ========================================================

    output = (
        "sample_archive_results.csv"
    )


    fieldnames = [
        "location_id",
        "station",
        "year",
        "file",
        "rows",
        "compressed_bytes",
        "first_datetime",
        "last_datetime",
        "parameters",
        "parameter_counts",
        "units",
        "sensor_count",
        "missing_total"
    ]


    with open(
        output,
        "w",
        newline="",
        encoding="utf-8"
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames
        )

        writer.writeheader()

        writer.writerows(
            all_samples
        )


    # ========================================================
    # SUMMARY
    # ========================================================

    if not all_samples:

        print(
            "No sample files were downloaded."
        )

        return


    total_rows = sum(
        int(x["rows"])
        for x in all_samples
    )


    total_bytes = sum(
        int(x["compressed_bytes"])
        for x in all_samples
    )


    average_rows = (
        total_rows
        /
        len(all_samples)
    )


    average_bytes = (
        total_bytes
        /
        len(all_samples)
    )


    print()
    print("=" * 70)
    print(
        "SAMPLING RESULTS"
    )
    print("=" * 70)


    print(
        "Sample files:",
        len(all_samples)
    )

    print(
        "Total sample rows:",
        total_rows
    )

    print(
        "Average rows/file:",
        round(
            average_rows,
            2
        )
    )

    print(
        "Average compressed size/file:",
        round(
            average_bytes / 1024,
            2
        ),
        "KB"
    )


    # ========================================================
    # ESTIMATE ENTIRE INVENTORY
    # ========================================================

    total_archive_files = sum(
        int(
            x["total_daily_files"]
        )
        for x in stations
    )


    estimated_rows = (
        total_archive_files
        *
        average_rows
    )


    print()
    print(
        "Total archive daily files "
        "(selected inventory):",
        total_archive_files
    )


    print(
        "Estimated measurement rows:",
        f"{estimated_rows:,.0f}"
    )


    print()
    print(
        "Saved:",
        output
    )


if __name__ == "__main__":

    main()