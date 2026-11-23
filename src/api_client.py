"""Сбор данных из OpenStreetMap Taginfo API и сохранение исходных данных."""
import csv
import json
import logging
import time
from datetime import datetime, timezone

import requests

from config import (API_URL, DATA_DIR, MAX_RETRIES, PAGE_SIZE, RAW_CSV,
                    RAW_JSON, REQUEST_TIMEOUT, TOTAL_RECORDS)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("api_client")


def fetch_page(page: int, page_size: int) -> dict:
    """Один запрос к API с повторами при ошибках сети и неуспешных статусах."""
    params = {
        "page": page,
        "rp": page_size,
        "sortname": "count_all",
        "sortorder": "desc",
    }
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(API_URL, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()  # HTTP 4xx/5xx -> исключение
            payload = response.json()
            if "data" not in payload:
                raise ValueError("В ответе API нет поля 'data'")
            return payload
        except (requests.RequestException, ValueError) as err:
            last_error = err
            log.warning("Страница %s, попытка %s/%s не удалась: %s",
                        page, attempt, MAX_RETRIES, err)
            time.sleep(2 * attempt)
    raise RuntimeError(f"Не удалось получить страницу {page}: {last_error}")


def collect_records(total: int = TOTAL_RECORDS, page_size: int = PAGE_SIZE) -> list[dict]:
    """Собирает `total` самых используемых ключей OSM (по count_all)."""
    records: list[dict] = []
    collected_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    page = 1
    while len(records) < total:
        payload = fetch_page(page, page_size)
        rows = payload["data"]
        if not rows:
            log.info("API вернул пустую страницу, останавливаемся.")
            break
        for row in rows:
            row["rank"] = len(records) + 1                  # место в рейтинге по count_all
            row["data_until"] = payload.get("data_until")   # дата актуальности данных Taginfo
            row["collected_at"] = collected_at              # когда мы собрали запись
            records.append(row)
            if len(records) >= total:
                break
        log.info("Страница %s получена, всего записей: %s", page, len(records))
        page += 1
        time.sleep(0.5)  # вежливая пауза для публичного API
    return records


def save_raw(records: list[dict]) -> None:
    """Сохраняем исходные данные ДО очистки (CSV + JSON)."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(RAW_JSON, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    fieldnames = sorted({k for r in records for k in r})
    with open(RAW_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    log.info("Исходные данные сохранены: %s, %s", RAW_CSV, RAW_JSON)


def load_raw() -> list[dict]:
    """Читает ранее сохранённые исходные записи (для producer)."""
    with open(RAW_JSON, encoding="utf-8") as f:
        return json.load(f)


if __name__ == "__main__":
    data = collect_records()
    save_raw(data)
    print(f"Собрано записей: {len(data)}")
    print("Пример:", data[0])
