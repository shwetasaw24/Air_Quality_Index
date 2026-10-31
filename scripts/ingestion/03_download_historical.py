import csv
import subprocess
from pathlib import Path
import time

INVENTORY = Path("metadata/openaq_archive_inventory.csv")

OUTPUT_DIR = Path("data/raw/openaq/historical")

FAILED_LOG = Path("metadata/download_failures.csv")

BUCKET = "openaq-data-archive"

MAX_RETRIES = 3


def download_file(s3_key, destination):

    destination.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    for attempt in range(1, MAX_RETRIES + 1):

        command = [
            "aws",
            "s3",
            "cp",
            f"s3://{BUCKET}/{s3_key}",
            str(destination),
            "--no-sign-request"
        ]

        result = subprocess.run(
            command,
            capture_output=True,
            text=True
        )

        if result.returncode == 0:

            if destination.exists() and destination.stat().st_size > 0:
                return True

        print(
            f"    Retry {attempt}/{MAX_RETRIES}"
        )

        if destination.exists():
            try:
                destination.unlink()
            except Exception:
                pass

        time.sleep(2)

    return False


def main():

    if not INVENTORY.exists():
        raise FileNotFoundError(
            f"Inventory not found: {INVENTORY}"
        )

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    failures = []

    with open(
        INVENTORY,
        "r",
        encoding="utf-8-sig"
    ) as f:

        rows = list(csv.DictReader(f))

    total = len(rows)

    print("=" * 70)
    print("OPENAQ BULK DOWNLOADER")
    print("=" * 70)
    print(f"Files in inventory: {total:,}")
    print(f"Destination: {OUTPUT_DIR}")
    print()

    downloaded = 0
    skipped = 0

    start_time = time.time()

    for i, row in enumerate(rows, 1):

        location_id = row["location_id"]
        year = row["year"]
        key = row["key"]

        filename = Path(key).name

        destination = (
            OUTPUT_DIR
            / f"location_{location_id}"
            / str(year)
            / filename
        )

        # Already downloaded
        if destination.exists():

            skipped += 1

        else:

            print(
                f"[{i:,}/{total:,}] "
                f"Downloading {filename}"
            )

            success = download_file(
                key,
                destination
            )

            if success:
                downloaded += 1

            else:

                print(
                    f"    FAILED: {key}"
                )

                failures.append([
                    location_id,
                    year,
                    key
                ])

        # Progress every 100 files
        if i % 100 == 0:

            elapsed = time.time() - start_time

            print()
            print("-" * 70)
            print(
                f"Progress: {i:,}/{total:,} "
                f"({i / total * 100:.1f}%)"
            )
            print(f"Downloaded: {downloaded:,}")
            print(f"Skipped: {skipped:,}")
            print(f"Failed: {len(failures):,}")
            print(
                f"Elapsed: {elapsed / 60:.1f} minutes"
            )
            print("-" * 70)
            print()

    # Save failures
    with open(
        FAILED_LOG,
        "w",
        newline="",
        encoding="utf-8"
    ) as f:

        writer = csv.writer(f)

        writer.writerow([
            "location_id",
            "year",
            "key"
        ])

        writer.writerows(failures)

    elapsed = time.time() - start_time

    print()
    print("=" * 70)
    print("DOWNLOAD COMPLETE")
    print("=" * 70)

    print(f"Inventory files : {total:,}")
    print(f"Downloaded      : {downloaded:,}")
    print(f"Skipped         : {skipped:,}")
    print(f"Failed          : {len(failures):,}")
    print(
        f"Time            : {elapsed / 60:.1f} minutes"
    )

    print()
    print(f"Failures saved: {FAILED_LOG}")


if __name__ == "__main__":
    main()