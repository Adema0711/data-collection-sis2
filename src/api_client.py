"""Collect population data from the World Bank API and save the raw records."""
import csv
import json
import logging
import time
from datetime import datetime, timezone

import requests

from config import (API_BASE_URL, COUNTRIES, DATA_DIR, DATE_RANGE, INDICATOR,
                    MAX_RETRIES, PER_PAGE, RAW_CSV, RAW_JSON, REQUEST_TIMEOUT)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("api_client")

COUNTRY_CODES = ";".join(COUNTRIES)


def get_json(url: str, params: dict) -> list:
    """GET request with retries. Returns the parsed World Bank response [meta, data]."""
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            payload = response.json()
            # The API reports some errors as HTTP 200 with a message instead of data
            if not isinstance(payload, list) or len(payload) < 2 or payload[1] is None:
                raise ValueError(f"Unexpected API response: {str(payload)[:200]}")
            return payload
        except (requests.RequestException, ValueError) as err:
            last_error = err
            log.warning("Attempt %s/%s failed: %s", attempt, MAX_RETRIES, err)
            time.sleep(2 * attempt)
    raise RuntimeError(f"Request failed after {MAX_RETRIES} attempts: {last_error}")


def fetch_country_info() -> dict:
    """Country metadata (region, income level) used as categorical fields."""
    url = f"{API_BASE_URL}/country/{COUNTRY_CODES}"
    _, items = get_json(url, {"format": "json", "per_page": 50})
    return {
        item["id"]: {
            "region": item["region"]["value"],
            "income_level": item["incomeLevel"]["value"],
        }
        for item in items
    }


def collect_records() -> list[dict]:
    """Population per country and year, joined with country metadata."""
    country_info = fetch_country_info()
    url = f"{API_BASE_URL}/country/{COUNTRY_CODES}/indicator/{INDICATOR}"
    collected_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    records: list[dict] = []
    page = 1
    while True:
        params = {"format": "json", "date": DATE_RANGE, "per_page": PER_PAGE, "page": page}
        meta, items = get_json(url, params)
        for item in items:
            code = item["countryiso3code"]
            info = country_info.get(code, {})
            records.append({
                "country_code": code,
                "country_name": item["country"]["value"],
                "region": info.get("region"),
                "income_level": info.get("income_level"),
                "year": item["date"],
                "population": item["value"],  # can be None if the year is not published yet
                "indicator_code": item["indicator"]["id"],
                "data_last_updated": meta.get("lastupdated"),
                "collected_at": collected_at,
            })
        log.info("Page %s/%s received, records so far: %s", page, meta["pages"], len(records))
        if page >= int(meta["pages"]):
            break
        page += 1
        time.sleep(0.5)
    return records


def save_raw(records: list[dict]) -> None:
    """Save the raw data before any cleaning (CSV + JSON)."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(RAW_JSON, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    with open(RAW_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(records[0].keys()))
        writer.writeheader()
        writer.writerows(records)
    log.info("Raw data saved: %s, %s", RAW_CSV, RAW_JSON)


def load_raw() -> list[dict]:
    """Read the saved raw records (used by the producer)."""
    with open(RAW_JSON, encoding="utf-8") as f:
        return json.load(f)


if __name__ == "__main__":
    data = collect_records()
    save_raw(data)
    missing = sum(1 for r in data if r["population"] is None)
    print(f"All records received: {len(data)}")
    print(f"Records with missing population: {missing}")
    print("Example:", data[0])
