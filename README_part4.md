# Part 4: SQLite database and full pipeline run

This part loads the cleaned data into SQLite, checks it with SQL queries, tests a repeated load of the same data, and runs the whole pipeline with one command.

## Run

```bash
docker compose up -d
python main.py
```

To start from a specific stage (for example, without calling the API again):

```bash
python main.py produce
python main.py clean
```

Stages: `collect`, `produce`, `consume`, `validate`, `clean`, `analyze`, `load`.

Only the database stage:

```bash
python src/loader.py
```

## Database schema (`sql/schema.sql`)

One table `population`, one row per country and year. The primary key is `(country_code, year)`, so the database cannot contain two rows for the same country and year.

| Field | Type |
|---|---|
| country_code | TEXT |
| country_name | TEXT |
| region | TEXT |
| income_level | TEXT |
| year | INTEGER |
| population | INTEGER |
| indicator_code | TEXT |
| data_last_updated | TEXT |
| collected_at | TEXT |

## Loading (`src/loader.py`)

- Reads `data/cleaned_data.csv` (output of `cleaner.py`).
- Creates a new database `data/population.db` on every run.
- Inserts rows with `INSERT OR IGNORE`: a row with a key that already exists is skipped.
- Prints the number of inserted and ignored rows.
- Saves the results to `data/load_report.json`.

## SQL checks (`sql/checks.sql`)

| Check | Expected |
|---|---|
| duplicate_keys | 0 |
| null_values | 0 |
| wrong_years (outside 2000-2024) | 0 |
| wrong_population (<= 0 or > 2 billion) | 0 |
| wrong_indicator (not SP.POP.TOTL) | 0 |
| countries_without_25_years | 0 |

## Double load test

After the first load, `loader.py` loads the same file again and checks:

- inserted rows = 0 (all 250 rows are ignored);
- number of rows stays 250;
- all rows before and after the second load are the same (data was not changed);
- duplicate keys = 0.

## Pipeline verification (`main.py`)

`main.py` runs all stages one by one. If a stage fails, the next stages are not run. At the end it reads the reports of all stages and builds `data/pipeline_verification.md`.

For every step the table shows how many records came in, how many came out, and the difference. If the difference is not 0, the comment explains why using the counters of that stage.

| Step | Compared counters | Possible reason of a difference |
|---|---|---|
| API -> Kafka producer | raw records / sent messages | messages that failed to send |
| producer -> topic | sent / messages in topic | old messages from previous producer runs |
| topic -> consumer | messages in topic / decoded | decode errors, Kafka errors, consumer stopped by timeout |
| consumer -> validation | decoded / received by validator | validator read a different file |
| validation | received / valid | invalid records, see `invalid_data.csv` |
| valid -> cleaning | valid / cleaner input | cleaner read a different file |
| cleaning | cleaner input / cleaned | missing values, wrong numbers, duplicates |
| cleaning -> database | cleaned / rows in database | rows ignored as duplicates |
| second load | rows before / rows after | second load added or changed rows |

Example: if `producer.py` was run twice, the topic has 500 messages instead of 250. The consumer reads all 500, and the validator rejects 250 of them as duplicates of `(country_code, year)`. So 250 records still reach the database.

## Expected results

- `loader.py` prints `Inserted: 250, ignored: 0`, all SQL checks are 0.
- Second load prints `Inserted: 0, ignored: 250` and `Data changed: False`.
- In `pipeline_verification.md` all steps are `OK`.

## Output files

| File | Content |
|---|---|
| `data/population.db` | SQLite database |
| `data/load_report.json` | load counters and SQL check results |
| `data/producer_report.json` | producer counters (saved by `main.py`) |
| `data/pipeline_verification.md` | pipeline verification table |