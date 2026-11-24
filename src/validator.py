"""Pandera validation of the records received from Kafka.

Stage order: consumer.py -> validator.py -> cleaner.py
Input : data/consumed_data.json
Output: data/valid_data.csv      - records that passed all rules (goes to cleaning)
        data/invalid_data.csv    - rejected records with the reason of every error
        data/validation_report.json
        data/validation_test_results.csv - demo with deliberately broken test records
"""
import json
import logging
import sys

import pandas as pd

try:
    import pandera.pandas as pa      # pandera >= 0.24
except ImportError:                  # older versions
    import pandera as pa

from config import (COUNTRIES, CONSUMED_JSON, DATA_DIR, INDICATOR, INVALID_CSV,
                    TEST_VALIDATION_CSV, VALID_CSV, VALIDATION_REPORT_JSON)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("validator")

FIELDS = ["country_code", "country_name", "region", "income_level", "year", "population",
          "indicator_code", "data_last_updated", "collected_at"]
INCOME_LEVELS = ["Low income", "Lower middle income", "Upper middle income", "High income"]
MIN_YEAR, MAX_YEAR = 2000, 2024
MAX_POPULATION = 2_000_000_000   # larger than any real country (India/China are ~1.4 billion)


def is_text(series: pd.Series) -> pd.Series:
    """True for every value that is a non-empty string."""
    return series.map(lambda v: isinstance(v, str) and v.strip() != "")


def is_number(series: pd.Series) -> pd.Series:
    """True for real numbers (bool and text are not accepted)."""
    return series.map(lambda v: isinstance(v, (int, float)) and not isinstance(v, bool))


def is_date(series: pd.Series) -> pd.Series:
    """True for strings that can be parsed as a date/time."""
    parsed = pd.to_datetime(series.astype(str), errors="coerce", utc=True, format="mixed")
    return parsed.notna()


def is_year_in_range(series: pd.Series) -> pd.Series:
    """True for 4-digit strings inside the requested period."""
    text = series.astype(str)
    numeric = pd.to_numeric(text.where(text.str.fullmatch(r"\d{4}")), errors="coerce")
    return numeric.between(MIN_YEAR, MAX_YEAR)


# Schema of one record. Data types are checked on the raw values (no coercion before validation).
# The data comes from JSON "as is", so the checks work on the raw values.
SCHEMA = pa.DataFrameSchema(
    columns={
        # Rule 1: required field + allowed categorical values
        "country_code": pa.Column(
            nullable=False,
            checks=[pa.Check(is_text, error="country_code must be a non-empty string"),
                    pa.Check.isin(COUNTRIES, error="country_code is not one of the 10 selected countries")]),
        # Rule 2: required text field
        "country_name": pa.Column(
            nullable=False,
            checks=pa.Check(is_text, error="country_name must be a non-empty string")),
        "region": pa.Column(
            nullable=False,
            checks=pa.Check(is_text, error="region must be a non-empty string")),
        # Rule 3: allowed categorical values
        "income_level": pa.Column(
            nullable=False,
            checks=pa.Check.isin(INCOME_LEVELS, error="income_level has an unknown value")),
        # Rule 4: year is a 4-digit value inside 2000-2024 (date validation)
        "year": pa.Column(
            nullable=False,
            checks=pa.Check(is_year_in_range, error=f"year must be a 4-digit value between {MIN_YEAR} and {MAX_YEAR}")),
        # Rule 5: population is a required number, 0 < value < 2 billion (type + range + missing)
        "population": pa.Column(
            nullable=False,
            checks=[pa.Check(is_number, error="population must be numeric"),
                    pa.Check(lambda s: pd.to_numeric(s, errors="coerce").between(1, MAX_POPULATION),
                             error=f"population must be between 1 and {MAX_POPULATION}")]),
        # Rule 6: fixed indicator code
        "indicator_code": pa.Column(
            nullable=False,
            checks=pa.Check.isin([INDICATOR], error=f"indicator_code must be {INDICATOR}")),
        # Rule 7: service dates must be valid date/time values
        "data_last_updated": pa.Column(
            nullable=False,
            checks=pa.Check(is_date, error="data_last_updated is not a valid date")),
        "collected_at": pa.Column(
            nullable=False,
            checks=pa.Check(is_date, error="collected_at is not a valid date/time")),
    },
    # Rule 8: the pair (country_code, year) must be unique; the first occurrence is kept
    unique=["country_code", "year"],
    report_duplicates="exclude_first",
    strict=False,   # unexpected keys are checked per record in validate_records()
    coerce=False,
)


def validate_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate all rules at once (lazy) and split the data into valid / invalid records."""
    df = df.reset_index(drop=True)
    try:
        SCHEMA.validate(df, lazy=True)
        valid = df.copy()
        invalid = df.iloc[0:0].copy()
        invalid["validation_errors"] = pd.Series(dtype=str)
        return valid, invalid
    except pa.errors.SchemaErrors as err:
        cases = err.failure_cases

    errors_by_row: dict[int, list[str]] = {}
    for _, case in cases.iterrows():
        check = str(case["check"])
        if check == "multiple_fields_uniqueness":
            message = "duplicate record: (country_code, year) is not unique"
        elif check == "not_nullable":
            message = f"{case['column']}: required value is missing"
        elif pd.notna(case["column"]):
            message = f"{case['column']}: {check} (value: {case['failure_case']})"
        else:
            message = f"{check} (value: {case['failure_case']})"
        if pd.isna(case["index"]):
            # Schema-level problem (for example a missing column) affects every row
            for row in range(len(df)):
                errors_by_row.setdefault(row, []).append(message)
        else:
            errors_by_row.setdefault(int(case["index"]), []).append(message)

    invalid_idx = sorted(errors_by_row)
    invalid = df.loc[invalid_idx].copy()
    invalid["validation_errors"] = [" | ".join(dict.fromkeys(errors_by_row[i])) for i in invalid_idx]
    valid = df.drop(index=invalid_idx)
    return valid, invalid


def validate_records(records: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Structure check (missing / unexpected keys per record) + Pandera schema validation."""
    structure_errors = {}
    for idx, record in enumerate(records):
        missing = [k for k in FIELDS if k not in record]
        unexpected = [k for k in record if k not in FIELDS]
        if missing or unexpected:
            structure_errors[idx] = f"record structure: missing keys {missing}, unexpected keys {unexpected}"
    df = pd.DataFrame(records).reindex(columns=FIELDS)
    valid, invalid = validate_dataframe(df)
    if structure_errors:
        moved = valid.loc[valid.index.intersection(list(structure_errors))].copy()
        moved["validation_errors"] = [structure_errors[i] for i in moved.index]
        valid = valid.drop(index=moved.index)
        for idx in invalid.index.intersection(list(structure_errors)):
            invalid.loc[idx, "validation_errors"] += " | " + structure_errors[idx]
        invalid = pd.concat([invalid, moved]).sort_index()
    return valid, invalid


def load_consumed() -> list[dict]:
    """Read the records saved by the consumer."""
    if not CONSUMED_JSON.exists():
        raise FileNotFoundError(f"{CONSUMED_JSON} not found. Run consumer.py first.")
    with open(CONSUMED_JSON, encoding="utf-8") as f:
        records = json.load(f)
    if not records:
        raise ValueError("The consumer saved no records.")
    return records


def build_test_records(valid: pd.DataFrame) -> pd.DataFrame:
    """Deliberately invalid test records. Each one is based on a different real record and then broken."""
    reference = valid.iloc[0]

    def broken(test_id: str, base: int, **changes) -> dict:
        row = valid.iloc[base].to_dict()
        row.update(changes)
        row["test_id"] = test_id
        return row

    return pd.DataFrame([
        reference.to_dict() | {"test_id": "CONTROL_valid"},                      # must be accepted
        broken("TEST_1_bad_values", 1, country_code="XXX", population=-5000),         # unknown country + negative population
        broken("TEST_2_bad_year_missing_name", 2, year="20x4", country_name=None),    # wrong year format + missing name
        broken("TEST_3_bad_category_date", 3, income_level="Super rich",
               indicator_code="WRONG.CODE", collected_at="not-a-date"),            # category, indicator, date
        broken("TEST_4_text_population", 4, population="many"),                       # wrong data type
        broken("TEST_5_duplicate_key", 0),                                         # same country_code + year as CONTROL
    ])


def run_validation_tests(valid: pd.DataFrame) -> dict:
    """Show that the schema detects errors. Test records are saved separately and never reach cleaning."""
    test_df = build_test_records(valid)
    test_ids = test_df["test_id"].reset_index(drop=True)
    good, bad = validate_dataframe(test_df[FIELDS])
    results = []
    for idx in range(len(test_df)):
        errors = bad.loc[idx, "validation_errors"] if idx in bad.index else ""
        results.append({"test_id": test_ids[idx], "detected_as_invalid": idx in bad.index,
                        "validation_errors": errors})
    result_df = pd.DataFrame(results)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    result_df.to_csv(TEST_VALIDATION_CSV, index=False, encoding="utf-8")

    print("\n--- Validation test with deliberately invalid records ---")
    for row in results:
        status = "REJECTED" if row["detected_as_invalid"] else "accepted"
        print(f"{row['test_id']}: {status}")
        if row["validation_errors"]:
            print(f"    {row['validation_errors']}")
    broken_total = len(test_df) - 1
    detected = int(result_df.loc[result_df.test_id != "CONTROL_valid", "detected_as_invalid"].sum())
    control_ok = not bool(result_df.loc[result_df.test_id == "CONTROL_valid", "detected_as_invalid"].iloc[0])
    print(f"Invalid test records detected: {detected} of {broken_total}; control record accepted: {control_ok}")
    print(f"Saved to: {TEST_VALIDATION_CSV} (test records are NOT part of valid_data.csv)")
    return {"test_records": broken_total, "test_records_detected": detected, "control_accepted": control_ok}


def run_validation() -> dict:
    """Validate the consumed data, save valid/invalid records and print the counters."""
    records = load_consumed()
    valid, invalid = validate_records(records)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    valid.to_csv(VALID_CSV, index=False, encoding="utf-8")
    invalid.to_csv(INVALID_CSV, index=False, encoding="utf-8")

    stats = {"received": len(records), "valid": len(valid), "invalid": len(invalid)}
    print(f"Records received for validation: {stats['received']}")
    print(f"Valid records: {stats['valid']}  -> {VALID_CSV}")
    print(f"Invalid records: {stats['invalid']}  -> {INVALID_CSV}")
    if len(invalid):
        print("Invalid records and reasons:")
        for _, row in invalid.iterrows():
            print(f"  {row.get('country_code')}_{row.get('year')}: {row['validation_errors']}")

    if len(valid):
        stats["tests"] = run_validation_tests(valid)
    with open(VALIDATION_REPORT_JSON, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)
    return stats


if __name__ == "__main__":
    try:
        run_validation()
    except (FileNotFoundError, ValueError) as err:
        log.error("%s", err)
        sys.exit(1)
