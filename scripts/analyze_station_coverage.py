import requests
import csv
import time
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


API_KEY = "6ee55ce45f46617c7f183777374711a508228d02bc6b53c5be6ee3e787a68a8e"

LOCATIONS_FILE = "data_india_locations.csv"
OUTPUT_FILE = "station_coverage.csv"

BASE_URL = "https://api.openaq.org/v3"

TARGET_PARAMETERS = {
    "pm25",
    "pm10",
    "no2",
    "so2",
    "o3",
    "co"
}


# --------------------------------------------------
# HTTP SESSION WITH RETRIES
# --------------------------------------------------

def create_session():

    session = requests.Session()

    retry_strategy = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=2,
        status_forcelist=[
            429,
            500,
            502,
            503,
            504
        ],
        allowed_methods=[
            "GET"
        ],
        raise_on_status=False
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy
    )

    session.mount(
        "https://",
        adapter
    )

    session.headers.update({
        "X-API-Key": API_KEY,
        "Accept": "application/json"
    })

    return session


# --------------------------------------------------
# GET LOCATION
# --------------------------------------------------

def get_location(session, location_id):

    url = f"{BASE_URL}/locations/{location_id}"

    try:

        response = session.get(
            url,
            timeout=(10, 60)
        )

        if response.status_code != 200:

            print(
                f"  API error for {location_id}: "
                f"{response.status_code}"
            )

            return None

        data = response.json()

        results = data.get(
            "results",
            []
        )

        if not results:
            print(
                f"  No data for location {location_id}"
            )
            return None

        return results[0]

    except requests.exceptions.RequestException as e:

        print(
            f"  Network error for {location_id}: "
            f"{type(e).__name__}"
        )

        return None


# --------------------------------------------------
# EXTRACT STATION INFORMATION
# --------------------------------------------------

def process_location(location):

    if location is None:
        return None

    # We want fixed stations
    if location.get("isMobile", True):
        return None

    sensors = location.get(
        "sensors",
        []
    )

    parameters = set()
    sensor_ids = []

    for sensor in sensors:

        sensor_id = sensor.get("id")

        if sensor_id is not None:
            sensor_ids.append(
                str(sensor_id)
            )

        parameter = sensor.get(
            "parameter",
            {}
        )

        parameter_name = parameter.get(
            "name"
        )

        if parameter_name:
            parameters.add(
                parameter_name.lower()
            )

    target_parameters = (
        parameters & TARGET_PARAMETERS
    )

    coordinates = location.get(
        "coordinates",
        {}
    )

    datetime_first = (
        location.get("datetimeFirst")
        or {}
    )

    datetime_last = (
        location.get("datetimeLast")
        or {}
    )

    return {

        "location_id":
            location.get("id"),

        "name":
            location.get("name"),

        "locality":
            location.get("locality"),

        "latitude":
            coordinates.get("latitude"),

        "longitude":
            coordinates.get("longitude"),

        "provider":
            (
                location.get("provider")
                or {}
            ).get("name"),

        "owner":
            (
                location.get("owner")
                or {}
            ).get("name"),

        "is_monitor":
            location.get("isMonitor"),

        "first_measurement_utc":
            datetime_first.get("utc"),

        "last_measurement_utc":
            datetime_last.get("utc"),

        "all_parameters":
            ",".join(
                sorted(parameters)
            ),

        "target_parameters":
            ",".join(
                sorted(target_parameters)
            ),

        "target_parameter_count":
            len(target_parameters),

        "sensor_count":
            len(sensor_ids),

        "sensor_ids":
            ",".join(sensor_ids)
    }


# --------------------------------------------------
# MAIN
# --------------------------------------------------

def main():

    if not os.path.exists(
        LOCATIONS_FILE
    ):

        print(
            f"File not found: "
            f"{LOCATIONS_FILE}"
        )

        return

    with open(
        LOCATIONS_FILE,
        "r",
        encoding="utf-8"
    ) as f:

        reader = csv.DictReader(f)

        locations = list(reader)

    print(
        f"Loaded {len(locations)} "
        f"Indian locations."
    )

    session = create_session()

    results = []

    failed = []

    for index, row in enumerate(
        locations,
        start=1
    ):

        location_id = row[
            "location_id"
        ]

        print(
            f"[{index}/{len(locations)}] "
            f"Checking {location_id}..."
        )

        location = get_location(
            session,
            location_id
        )

        if location is None:

            failed.append(
                location_id
            )

            continue

        station = process_location(
            location
        )

        if station is not None:

            results.append(
                station
            )

        # Be gentle with API
        time.sleep(0.4)


    # --------------------------------------------------
    # RANK
    # --------------------------------------------------

    results.sort(
        key=lambda x: (
            x["target_parameter_count"],
            x["sensor_count"],
            x["first_measurement_utc"]
            or ""
        ),
        reverse=True
    )


    # --------------------------------------------------
    # SAVE
    # --------------------------------------------------

    fieldnames = [

        "location_id",
        "name",
        "locality",

        "latitude",
        "longitude",

        "provider",
        "owner",

        "is_monitor",

        "first_measurement_utc",
        "last_measurement_utc",

        "all_parameters",
        "target_parameters",

        "target_parameter_count",
        "sensor_count",

        "sensor_ids"
    ]

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

        writer.writerows(
            results
        )


    # --------------------------------------------------
    # SUMMARY
    # --------------------------------------------------

    print()
    print("=" * 60)
    print("STATION ANALYSIS COMPLETED")
    print("=" * 60)

    print(
        "Locations checked:",
        len(locations)
    )

    print(
        "Valid fixed stations:",
        len(results)
    )

    print(
        "Failed requests:",
        len(failed)
    )

    print(
        "Output:",
        OUTPUT_FILE
    )


    # --------------------------------------------------
    # TOP STATIONS
    # --------------------------------------------------

    print()
    print("TOP 20 STATIONS")
    print("-" * 60)

    for rank, station in enumerate(
        results[:20],
        start=1
    ):

        print(
            f"{rank:2}. "
            f"{station['location_id']} | "
            f"{station['name']} | "
            f"{station['first_measurement_utc']} → "
            f"{station['last_measurement_utc']} | "
            f"{station['target_parameters']}"
        )


    # --------------------------------------------------
    # FAILED IDS
    # --------------------------------------------------

    if failed:

        print()
        print(
            "FAILED LOCATION IDS:"
        )

        print(
            ", ".join(
                map(str, failed)
            )
        )


if __name__ == "__main__":
    main()