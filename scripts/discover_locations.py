import requests
import csv
import time

API_KEY = "6ee55ce45f46617c7f183777374711a508228d02bc6b53c5be6ee3e787a68a8e"

URL = "https://api.openaq.org/v3/locations"

HEADERS = {
    "X-API-Key": API_KEY
}

PAGE_SIZE = 100
MAX_PAGES = 50

india_locations = []

for page in range(1, MAX_PAGES + 1):

    print(f"Fetching page {page}...")

    params = {
        "limit": PAGE_SIZE,
        "page": page
    }

    response = requests.get(
        URL,
        headers=HEADERS,
        params=params,
        timeout=30
    )

    if response.status_code != 200:
        print("Request failed")
        print(response.status_code)
        print(response.text)
        break

    data = response.json()

    results = data.get("results", [])

    if not results:
        print("No more locations found.")
        break

    for location in results:

        country = location.get("country", {})

        if country.get("code") == "IN":

            coordinates = location.get("coordinates") or {}

            sensors = location.get("sensors") or []

            parameters = []

            for sensor in sensors:

                parameter = sensor.get("parameter") or {}

                parameter_name = parameter.get("name")

                if parameter_name:
                    parameters.append(parameter_name)

            india_locations.append({
                "location_id": location.get("id"),
                "name": location.get("name"),
                "locality": location.get("locality"),
                "country": country.get("code"),
                "latitude": coordinates.get("latitude"),
                "longitude": coordinates.get("longitude"),
                "is_mobile": location.get("isMobile"),
                "is_monitor": location.get("isMonitor"),
                "parameters": ",".join(sorted(set(parameters))),
                "datetime_first": location.get("datetimeFirst"),
                "datetime_last": location.get("datetimeLast")
            })

    print(
        f"India locations found so far: "
        f"{len(india_locations)}"
    )

    # Avoid hammering the API
    time.sleep(0.2)


# Remove duplicate location IDs
unique_locations = {}

for location in india_locations:
    unique_locations[location["location_id"]] = location

india_locations = list(unique_locations.values())


# Save result
output_file = "data_india_locations.csv"

fieldnames = [
    "location_id",
    "name",
    "locality",
    "country",
    "latitude",
    "longitude",
    "is_mobile",
    "is_monitor",
    "parameters",
    "datetime_first",
    "datetime_last"
]

with open(
    output_file,
    "w",
    newline="",
    encoding="utf-8"
) as f:

    writer = csv.DictWriter(
        f,
        fieldnames=fieldnames
    )

    writer.writeheader()
    writer.writerows(india_locations)


print("\n====================================")
print("Discovery completed")
print("India locations:", len(india_locations))
print("Saved to:", output_file)
print("====================================")