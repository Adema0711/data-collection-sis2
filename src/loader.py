import json
import sqlite3

import pandas as pd

from config import BASE_DIR, DATA_DIR

DB_PATH = DATA_DIR / "population.db"
CLEANED_CSV = DATA_DIR / "cleaned_data.csv"
SCHEMA_SQL = BASE_DIR / "sql" / "schema.sql"
CHECKS_SQL = BASE_DIR / "sql" / "checks.sql"
LOAD_REPORT = DATA_DIR / "load_report.json"

COLUMNS = ["country_code", "country_name", "region", "income_level", "year",
           "population", "indicator_code", "data_last_updated", "collected_at"]


def count_rows(conn):
    return conn.execute("SELECT COUNT(*) FROM population").fetchone()[0]


def get_all_rows(conn):
    return conn.execute("SELECT * FROM population ORDER BY country_code, year").fetchall()


def load_csv(conn):
    df = pd.read_csv(CLEANED_CSV)
    rows = df[COLUMNS].astype(object).values.tolist()
    before = count_rows(conn)
    conn.executemany("INSERT OR IGNORE INTO population VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.commit()
    after = count_rows(conn)
    return {
        "rows_in_file": len(rows),
        "rows_before": before,
        "inserted": after - before,
        "ignored": len(rows) - (after - before),
        "rows_after": after,
    }


def run_checks(conn):
    with open(CHECKS_SQL, encoding="utf-8") as f:
        queries = f.read().split(";")
    results = {}
    for query in queries:
        if query.strip():
            name, value = conn.execute(query).fetchone()
            results[name] = value
    return results


def run_loader():
    if DB_PATH.exists():
        DB_PATH.unlink()
    conn = sqlite3.connect(DB_PATH)
    with open(SCHEMA_SQL, encoding="utf-8") as f:
        conn.executescript(f.read())

    report = load_csv(conn)
    print(f"Rows in cleaned_data.csv: {report['rows_in_file']}")
    print(f"Inserted: {report['inserted']}, ignored: {report['ignored']}")
    print(f"Rows in database: {report['rows_after']}")

    checks = run_checks(conn)
    print("\nSQL checks:")
    for name, value in checks.items():
        print(f"  {name}: {value} {'OK' if value == 0 else 'ERROR'}")
    report["checks"] = checks

    print("\nSecond load of the same file:")
    data_before = get_all_rows(conn)
    second = load_csv(conn)
    data_after = get_all_rows(conn)
    report["second_load_inserted"] = second["inserted"]
    report["rows_after_second_load"] = second["rows_after"]
    report["data_changed"] = data_before != data_after
    print(f"  Inserted: {second['inserted']}, ignored: {second['ignored']}")
    print(f"  Rows: {second['rows_before']} -> {second['rows_after']}")
    print(f"  Data changed: {report['data_changed']}")
    print(f"  Duplicates: {run_checks(conn)['duplicate_keys']}")
    conn.close()

    with open(LOAD_REPORT, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    return report


if __name__ == "__main__":
    run_loader()