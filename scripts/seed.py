import pandas as pd
import numpy as np
import time
import re
import datetime
from collections import defaultdict
from scripts.db import get_connection


TARGET_CITIES = {
    # "bozeman"
    # # Original
    "munich",
    "amsterdam",
    "barcelona",
    "berlin",
    "paris",
    "london",
    "rome",
    "vienna",
    "dublin",
    "sydney",
    # # US
    "new-york-city",
    "los-angeles",
    # "chicago",
    # "san-francisco",
    # "austin",
    # "boston",
    # "seattle",
    # "san-diego",
    # "denver",
    # "nashville",
    # "portland",
    # "washington-dc",
    # "hawaii",
    # "clark-county-nv",
    # "jersey-city",
    # "cambridge",
    # "columbus",
    # "dallas",
    # "new-orleans",
    # "oakland",
    # "rhode-island",
    # "salem-or",
    # "san-mateo-county",
    # "santa-clarita",
    # "santa-cruz-county",
    # "twin-cities-msa",
    # "asheville",
    # # Canada
    "toronto",
    "vancouver",
    # "montreal",
    # "victoria",
    "quebec-city",
    # "new-brunswick",
    # "winnipeg",
    # More Europe
    "madrid",
    "milan",
    "lisbon",
    "prague",
    "budapest",
    "athens",
    "copenhagen",
    "stockholm",
    "oslo",
    "brussels",
    "venice",
    "florence",
    "naples",
    "geneva",
    "zurich",
}


def parse_html_for_links(html_file_path):
    print(f"Parsing HTML file for download links: {html_file_path}")
    with open(html_file_path, "r", encoding="utf-8") as f:
        html_content = f.read()

    # Match URLs with either .csv or .csv.gz
    pattern = r'href=[\'"](https?://data\.insideairbnb\.com/([^/]+)/([^/]+)/([^/]+)/([^/]+)/(?:data|visualisations)/(listings|calendar|reviews|neighbourhoods)\.csv(\.gz)?)[\'"]'
    matches = re.findall(pattern, html_content)

    # city_key -> { date_str, files: { filename: url, filename_is_gz: bool } }
    cities = {}
    for match in set(matches):
        url, country, state, city, date_str, file_basename, gz_ext = match

        if city.lower() not in TARGET_CITIES:
            continue

        city_key = (country, state, city)
        if city_key not in cities:
            cities[city_key] = {"date": date_str, "files": {}}
        elif date_str > cities[city_key]["date"]:
            cities[city_key]["date"] = date_str
            cities[city_key]["files"] = {}

        # We prefer the .gz version (detailed data) if multiple are found.
        # If we already stored a .gz version, don't overwrite it with a .csv version.
        existing_url = cities[city_key]["files"].get(file_basename, "")
        if existing_url.endswith(".gz") and not url.endswith(".gz"):
            continue

        cities[city_key]["files"][file_basename] = url

    return cities


def safe_float(val):
    if pd.isna(val):
        return None
    return float(val)


def safe_int(val):
    if pd.isna(val):
        return None
    return int(float(val))


def safe_str(val, max_len=None):
    if pd.isna(val):
        return None
    s = str(val).strip()
    if max_len and len(s) > max_len:
        return s[:max_len]
    return s


def safe_date(val):
    if pd.isna(val):
        return None
    # Assuming YYYY-MM-DD
    try:
        return pd.to_datetime(val).to_pydatetime()
    except:
        return None


def clean_price(val):
    if pd.isna(val):
        return None
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(str(val).replace("$", "").replace(",", "").strip())
    except:
        return None


def clean_boolean(val):
    if pd.isna(val):
        return None
    val = str(val).strip().lower()
    return 1 if val in ["t", "true", "1"] else 0


def insert_data(cursor, table, columns, data):
    if not data:
        print(f"  [~] No data to insert into {table}.")
        return

    print(f"  [+] Preparing to insert {len(data)} rows into {table}...")
    start_t = time.time()

    placeholders = ", ".join([f":{i+1}" for i in range(len(columns))])
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"

    # Oracle executemany works best with lists of tuples
    try:
        cursor.executemany(sql, data)
        print(
            f"  [+] Inserted {len(data)} rows into {table} in {time.time() - start_t:.2f}s"
        )
    except Exception as e:
        print(f"  [!] Warning inserting to {table}: {e}")
        # Normally you'd want batcherrors=True and log specifically, omitted for brevity
        pass


def process_city(conn, location_id, country, state, city, data_info):
    cursor = conn.cursor()
    print(f"\n--- Processing {city}, {country} (ID: {location_id}) ---")

    # 1. Insert Location
    insert_data(
        cursor,
        "locations",
        ["location_id", "location_name", "country", "state_code"],
        [
            (
                location_id,
                safe_str(city, 255),
                safe_str(country, 255),
                safe_str(state, 255),
            )
        ],
    )
    conn.commit()

    files = data_info["files"]
    if "neighbourhoods" not in files or "listings" not in files:
        print("  [-] Missing required files (neighbourhoods or listings), skipping...")
        return

    # 2. Neighbourhoods
    print(f"  [*] Downloading {files['neighbourhoods']} ...")
    start_dl_n = time.time()
    neigh_df = pd.read_csv(files["neighbourhoods"])
    print(
        f"  [*] Downloaded {len(neigh_df)} neighbourhoods in {time.time() - start_dl_n:.2f}s"
    )

    neigh_map = {}
    neigh_data = []
    # Fetching latest neighborhood ID to increment
    cursor.execute("SELECT NVL(MAX(neighbourhood_id), 0) FROM neighbourhoods")
    n_id = cursor.fetchone()[0]

    for idx, row in neigh_df.iterrows():
        n_id += 1
        n_name = str(row["neighbourhood"]).strip()
        n_group = (
            safe_str(row.get("neighbourhood_group"), 255)
            if "neighbourhood_group" in row
            else None
        )

        neigh_map[n_name] = n_id
        neigh_data.append((n_id, location_id, n_group, safe_str(n_name, 255)))

    insert_data(
        cursor,
        "neighbourhoods",
        ["neighbourhood_id", "location_id", "neighbourhood_group", "neighbourhood"],
        neigh_data,
    )
    conn.commit()

    # 3. Listings, Hosts, and Snapshots
    print(f"  [*] Downloading {files['listings']} (this might take a while)...")
    start_dl_l = time.time()

    listings_df = pd.read_csv(files["listings"], low_memory=False)

    print(
        f"  [*] Downloaded {len(listings_df)} listings in {time.time() - start_dl_l:.2f}s"
    )

    print("  [*] Processing Hosts...")
    # Clean unique hosts
    hosts_df = listings_df.drop_duplicates(subset=["host_id"]).copy()
    hosts_data = []
    for _, row in hosts_df.iterrows():
        hosts_data.append(
            (
                safe_int(row["host_id"]),
                safe_str(row.get("host_name"), 255) or "Unknown",
                safe_date(row.get("host_since")) or datetime.datetime(2000, 1, 1),
                safe_str(row.get("host_location"), 255) or "Unknown",
                safe_str(row.get("host_about"), 4000),
                safe_str(row.get("host_response_time"), 255),
                safe_str(row.get("host_response_rate"), 255),
                safe_str(row.get("host_acceptance_rate"), 255),
                clean_boolean(row.get("host_is_superhost")) or 0,
                safe_int(row.get("host_total_listings_count")) or 0,
            )
        )
    print(f"Inserting {len(hosts_data)} hosts...")
    insert_data(
        cursor,
        "hosts",
        [
            "host_id",
            "host_name",
            "host_since",
            "host_location",
            "host_about",
            "host_response_time",
            "host_response_rate",
            "host_acceptance_rate",
            "host_is_superhost",
            "host_total_listings_count",
        ],
        hosts_data,
    )
    conn.commit()

    # Listings
    listings_data = []
    snapshot_data = []
    scrape_id = int(time.time())  # Mocking Scrape ID based on timestamp
    scrape_date = safe_date(data_info["date"]) or datetime.datetime.now()

    valid_listing_ids = set()
    listing_base_prices = {}

    for _, row in listings_df.iterrows():
        neigh_name = str(
            row.get("neighbourhood_cleansed", row.get("neighbourhood", ""))
        ).strip()
        n_id_mapped = neigh_map.get(neigh_name)
        if not n_id_mapped:
            continue  # Skip records with unknown neighbourhoods

        l_id = safe_int(row["id"])
        valid_listing_ids.add(l_id)
        listing_base_prices[l_id] = clean_price(row.get("price")) or 0.0
        listings_data.append(
            (
                l_id,
                safe_int(row["host_id"]),
                safe_str(row.get("listing_url"), 1024),
                safe_str(row.get("name", "Unknown"), 255) or "Unknown",
                safe_str(row.get("description"), 4000),
                safe_str(row.get("picture_url"), 1024),
                n_id_mapped,
                safe_float(row.get("latitude")),
                safe_float(row.get("longitude")),
                safe_str(row.get("property_type"), 255),
                safe_str(row.get("room_type", "Unknown"), 255) or "Unknown",
                safe_int(row.get("accommodates")),
                safe_str(row.get("bathrooms_text"), 255),
                safe_int(row.get("bedrooms")),
                safe_int(row.get("beds")),
                safe_str(row.get("license"), 255),
                clean_boolean(row.get("instant_bookable")),
            )
        )

        snapshot_data.append(
            (
                l_id,
                scrape_id,
                scrape_date,
                clean_price(row.get("price")),
                safe_int(row.get("minimum_nights")),
                safe_int(row.get("number_of_reviews")),
                safe_date(row.get("last_review")),
                safe_float(row.get("reviews_per_month")),
                safe_int(row.get("availability_365")),
                safe_int(row.get("number_of_reviews_ltm")),
            )
        )

    print(f"Inserting {len(listings_data)} listings and snapshots...")
    insert_data(
        cursor,
        "listings",
        [
            "listing_id",
            "host_id",
            "listing_url",
            "name",
            "description",
            "picture_url",
            "neighbourhood_id",
            "latitude",
            "longitude",
            "property_type",
            "room_type",
            "accommodates",
            "bathrooms_text",
            "bedrooms",
            "beds",
            "license",
            "instant_bookable",
        ],
        listings_data,
    )

    insert_data(
        cursor,
        "listing_scrape_snapshot",
        [
            "listing_id",
            "scrape_id",
            "scraped_at",
            "price",
            "minimum_nights",
            "number_of_reviews",
            "last_review",
            "reviews_per_month",
            "availability_365",
            "number_of_reviews_ltm",
        ],
        snapshot_data,
    )
    conn.commit()

    # 4. Processing Calendar
    if "calendar" in files:
        print(f"  [*] Downloading and chunking {files['calendar']}...")
        cursor.execute("SELECT NVL(MAX(calendar_id), 0) FROM calendar")
        cal_id = cursor.fetchone()[0]

        comp = "gzip" if files["calendar"].endswith(".gz") else None
        cal_iter = pd.read_csv(
            files["calendar"], compression=comp, chunksize=1000000, low_memory=False
        )

        total_cal = 0
        for chunk in cal_iter:
            cal_data = []
            for _, row in chunk.iterrows():
                l_id = safe_int(row.get("listing_id"))
                if l_id not in valid_listing_ids:
                    continue

                raw_date = str(row.get("date", ""))
                if "2025-12" not in raw_date:
                    continue

                cal_id += 1

                calendar_price = clean_price(row.get("price"))
                if calendar_price is None or calendar_price == 0.0:
                    calendar_price = listing_base_prices.get(l_id, 0.0)

                cal_data.append(
                    (
                        cal_id,
                        l_id,
                        safe_date(row.get("date")),
                        clean_boolean(row.get("available", "f")),
                        calendar_price,
                        safe_int(row.get("minimum_nights")) or 1,
                        safe_int(row.get("maximum_nights")) or 1,
                    )
                )

            if cal_data:
                insert_data(
                    cursor,
                    "calendar",
                    [
                        "calendar_id",
                        "listing_id",
                        '"date"',
                        "available",
                        "price",
                        "minimum_nights",
                        "maximum_nights",
                    ],
                    cal_data,
                )
                conn.commit()
                total_cal += len(cal_data)
        print(f"  [*] Finished processing {total_cal} calendar rows.")

    # 5. Processing Reviews
    if "reviews" in files:
        print(f"  [*] Downloading and chunking {files['reviews']}...")
        comp = "gzip" if files["reviews"].endswith(".gz") else None
        # We assume reviews.csv.gz has an 'id' column we can use for review_id, otherwise we auto-increment
        cursor.execute("SELECT NVL(MAX(review_id), 0) FROM reviews")
        rev_id_gen = cursor.fetchone()[0]

        rev_iter = pd.read_csv(
            files["reviews"], compression=comp, chunksize=50000, low_memory=False
        )

        total_rev = 0
        for chunk in rev_iter:
            rev_data = []
            for _, row in chunk.iterrows():
                l_id = safe_int(row.get("listing_id"))
                if l_id not in valid_listing_ids:
                    continue

                r_id = safe_int(row.get("id"))
                if r_id is None:
                    rev_id_gen += 1
                    r_id = rev_id_gen

                rev_data.append(
                    (
                        r_id,
                        l_id,
                        safe_date(row.get("date")),
                        safe_int(row.get("reviewer_id")) or 0,
                        safe_str(row.get("reviewer_name"), 255) or "Unknown",
                        safe_str(row.get("comments"), 4000),
                    )
                )

            if rev_data:
                insert_data(
                    cursor,
                    "reviews",
                    [
                        "review_id",
                        "listing_id",
                        '"date"',
                        "reviewer_id",
                        "reviewer_name",
                        "comments",
                    ],
                    rev_data,
                )
                conn.commit()
                total_rev += len(rev_data)
        print(f"  [*] Finished processing {total_rev} review rows.")

    print(f"Finished seeding {city}.")


def run_seed():
    print("--- Starting Database Seeder ---")
    html_path = r"Get the Data _ Inside Airbnb.html"

    try:
        cities = parse_html_for_links(html_path)
        print(f"Discovered {len(cities)} unique city datasets.")

        conn = get_connection()

        # Start setting up location sequence
        cursor = conn.cursor()
        cursor.execute("SELECT NVL(MAX(location_id), 0) FROM locations")
        location_id = cursor.fetchone()[0]

        limit_cities = (
            10  # Reduced to only process the hardcoded TARGET_CITIES (10 total)
        )

        overall_start_time = time.time()

        for idx, (city_key, data_info) in enumerate(cities.items()):
            if idx >= limit_cities:
                print(
                    f"\n[!] Reached limit of {limit_cities} cities. Set higher in script to process more."
                )
                break

            country, state, city = city_key
            location_id += 1

            try:
                city_start_t = time.time()
                process_city(conn, location_id, country, state, city, data_info)
                print(
                    f"--- Finished processing {city} in {time.time() - city_start_t:.2f}s ---\n"
                )
            except Exception as inner_e:
                print(f"[!] Error processing {city}: {inner_e}")

        print(
            f"\nSeeding logic finished successfully in {time.time() - overall_start_time:.2f}s."
        )

    except Exception as e:
        print(f"[!] Error during seeding: {e}")


if __name__ == "__main__":
    run_seed()
