# World Bank Population: Kafka + Pandera + SQLite Pipeline

End-to-end pipeline: World Bank API -> Kafka (producer/consumer) -> Pandera validation -> pandas/NumPy cleaning -> SQLite -> EDA and figures.

## Requirements
- Python 3.10+ (tested with 3.12)
- Docker Desktop / Docker Engine (runs the Kafka broker)
- Internet access (public World Bank API, no API key needed)

## Setup
```bash
python -m venv .venv
source .venv/bin/activate          # Linux / macOS
.venv\Scripts\activate             # Windows
pip install -r requirements.txt
docker compose up -d               # starts Kafka broker on localhost:9092
```

## API and Kafka configuration
- API: `GET https://api.worldbank.org/v2/country/{codes}/indicator/SP.POP.TOTL`, params `format=json`, `date=2000:2024`, `per_page=500`, `page`. Country metadata (region, income level) comes from `GET /v2/country/{codes}`. 10 countries x 25 years = 250 records.
- Kafka: container `world_bank`, broker `localhost:9092`, topic `worldbank_population` (1 partition, replication 1), created automatically by the producer. Settings are in `src/config.py` and can be overridden with `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID`.

## Run the whole pipeline
```bash
python main.py
```
Stages: `collect, produce, consume, validate, clean, analyze, load`. To start from a stage: `python main.py produce`. Before `produce`, `main.py` deletes the old topic so that every run starts with an empty topic.

## Run stages separately (execution order)
```bash
cd src
python api_client.py    # API -> data/raw_data.csv, data/raw_data.json (250 records)
python producer.py      # raw_data.json -> Kafka topic
python consumer.py      # Kafka topic -> data/consumed_data.json
python validator.py     # Pandera: valid_data.csv, invalid_data.csv, validation tests
python cleaner.py       # valid_data.csv -> cleaned_data.csv
python analysis.py      # cleaned_data.csv -> analysis/ (tables, 3 figures, findings.md)
cd ..
python src/loader.py    # cleaned_data.csv -> data/population.db (+ repeated-load test)
```
Note: running `producer.py` alone twice leaves 500 messages in the topic; the validator then rejects the 250 duplicates of `(country_code, year)`.

## Modules
| File | Purpose |
|---|---|
| `src/api_client.py` | API requests with timeout and 3 retries, saves raw data |
| `src/producer.py` | creates topic, sends records as JSON, counts delivered messages |
| `src/consumer.py` | reads the topic, deserializes JSON; stops when all messages are read or after 15 s idle |
| `src/validator.py` | Pandera schema (8 rules), invalid records with reasons, 5 deliberately invalid test records |
| `src/cleaner.py` | cleaning and type conversion, removal of missing/duplicate rows |
| `src/loader.py` | SQLite loading (`sql/schema.sql`), SQL checks (`sql/checks.sql`), double-load test |
| `src/analysis.py` | EDA: shape, dtypes, describe, categorical and grouped statistics, 3 figures |
| `main.py` | runs all stages and builds `data/pipeline_verification.md` |

## Validation (Pandera)
Rules: required fields; allowed `country_code` / `income_level` / `indicator_code`; data types; year 4-digit in 2000-2024; 1 <= population <= 2 billion; valid dates; unique `(country_code, year)`; record structure. Invalid records go to `data/invalid_data.csv` with the reason; only `data/valid_data.csv` goes to cleaning. Test records (`data/validation_test_results.csv`) never reach the cleaned data.

## Database
Table `population`, primary key `(country_code, year)`, loaded with `INSERT OR IGNORE`. Loading the same file a second time inserts 0 rows and does not change the data.

## Expected outputs
- `api_client.py`: `All records received: 250`
- `producer.py`: `Successfully published: 250`, `Submission errors: 0`
- `consumer.py`: `Successfully consumed: 250`, `Decoding errors: 0`
- `validator.py`: valid 250 / invalid 0; test records detected 5 of 5, control accepted
- `cleaner.py`: `output_rows: 250`
- `loader.py`: `Inserted: 250, ignored: 0`; second load `Inserted: 0, ignored: 250`, `Data changed: False`; all SQL checks 0
- `analysis/`: 3 PNG figures, CSV tables, `findings.md`

## Verify a successful run
1. `data/pipeline_verification.md`: every stage is `OK`, difference 0.
2. Kafka message count: `docker exec world_bank /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic worldbank_population` prints `worldbank_population:0:250`.
3. Database: `sqlite3 data/population.db "SELECT COUNT(*) FROM population;"` returns 250.

Stop Kafka: `docker compose down`.