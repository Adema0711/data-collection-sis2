"""Clean the validated population records and count each removal."""
import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from config import DATA_DIR, DATE_RANGE, VALID_CSV

CLEANED_CSV = DATA_DIR / "cleaned_data.csv"
KEY = ["country_code", "year"]
TEXT_COLUMNS = [
    "country_code", "country_name", "region", "income_level",
    "indicator_code", "data_last_updated", "collected_at",
]
NUMBER_COLUMNS = ["year", "population"]
START_YEAR, END_YEAR = map(int, DATE_RANGE.split(":"))


def run_cleaning(source=VALID_CSV, output=CLEANED_CSV):
    source, output = Path(source), Path(output)
    columns = TEXT_COLUMNS + NUMBER_COLUMNS
    data = pd.read_csv(source, dtype="string", keep_default_na=False)
    missing_columns = set(columns) - set(data)
    if missing_columns:
        raise ValueError(f"Missing columns: {', '.join(sorted(missing_columns))}")

    report = {"source": source.name, "input_rows": len(data)}
    data = data.loc[:, columns].apply(lambda column: column.str.strip())
    data["country_code"] = data["country_code"].str.upper()
    incomplete = data.eq("").any(axis=1)
    report["missing_rows"] = int(incomplete.sum())
    data = data.loc[~incomplete].copy()

    for column in NUMBER_COLUMNS:
        data[column] = pd.to_numeric(data[column], errors="coerce")
    numbers = data[NUMBER_COLUMNS]
    valid = np.isfinite(numbers).all(axis=1) & numbers.mod(1).eq(0).all(axis=1)
    valid &= data["year"].between(START_YEAR, END_YEAR)
    valid &= data["population"].between(1, 2_000_000_000)
    valid = valid.fillna(False)
    report["invalid_numeric_rows"] = int((~valid).sum())
    data = data.loc[valid].astype(dict.fromkeys(NUMBER_COLUMNS, "int64"))

    duplicates = data.duplicated(KEY)
    report["duplicate_rows"] = int(duplicates.sum())
    data = data.loc[~duplicates].sort_values(KEY)
    report["output_rows"] = len(data)
    output.parent.mkdir(parents=True, exist_ok=True)
    data.to_csv(output, index=False)
    output.with_name("cleaning_report.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, indent=2))
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=VALID_CSV)
    parser.add_argument("--output", type=Path, default=CLEANED_CSV)
    args = parser.parse_args()
    if not args.input.is_file():
        parser.error(f"{args.input} is missing. Run validator.py or provide --input.")
    run_cleaning(args.input, args.output)
