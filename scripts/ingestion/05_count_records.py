import gzip
import csv
import glob
import os
import time

files = glob.glob(
    r"data\raw\openaq\historical\**\*.csv.gz",
    recursive=True
)

total_rows = 0
total_files = len(files)

print("=" * 60)
print("OpenAQ Dataset Size Check")
print("=" * 60)
print(f"CSV files found: {total_files:,}")
print()

start = time.time()

for i, file in enumerate(files, 1):

    try:
        with gzip.open(file, "rt", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)

            # Skip header
            next(reader, None)

            rows = sum(1 for _ in reader)
            total_rows += rows

    except Exception as e:
        print(f"ERROR: {file}")
        print(e)

    if i % 1000 == 0 or i == total_files:
        elapsed = time.time() - start
        print(
            f"Processed {i:,}/{total_files:,} files | "
            f"Rows: {total_rows:,} | "
            f"Time: {elapsed/60:.1f} min"
        )

print()
print("=" * 60)
print("FINAL RESULT")
print("=" * 60)
print(f"Files       : {total_files:,}")
print(f"Measurements: {total_rows:,}")
print(f"Time        : {(time.time()-start)/60:.1f} minutes")
print("=" * 60)