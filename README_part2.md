# Part 2: Kafka Consumer and Pandera Validation

(Add this section to README.md after Part 1.)

This part reads all messages from the Kafka topic, deserializes the JSON, and validates the records with Pandera before cleaning.

## Run (after `api_client.py` and `producer.py`)

```bash
cd src
python consumer.py      # Kafka -> data/consumed_data.json
python validator.py     # data/consumed_data.json -> data/valid_data.csv + data/invalid_data.csv
```

## Consumer (`src/consumer.py`)
- Group id `worldbank_consumer_group`; every run reads the topic from the first message.
- JSON deserialization with `json.loads`; messages that cannot be decoded are counted and logged, not fatal.
- Errors handled: broker unavailable, topic missing, Kafka message errors, invalid JSON.
- **Termination condition:** the consumer stops when it has read all messages that were in the topic at start (end offset), or after 15 s without new messages (`CONSUMER_IDLE_TIMEOUT` in `config.py`).
- Prints: messages in topic, consumed, decoded, decoding errors, Kafka errors, stop reason.
- Expected: `Successfully consumed: 250`, `Decoding errors: 0`.

## Validator (`src/validator.py`)
Rules in the Pandera schema (all checked at once, `lazy=True`):

| # | Rule | Fields |
|---|---|---|
| 1 | Required fields, no missing values | all fields |
| 2 | Allowed categorical values | `country_code` (10 countries), `income_level` (4 World Bank groups), `indicator_code` (`SP.POP.TOTL`) |
| 3 | Data type (string / number) | text fields, `population` |
| 4 | Year is a 4-digit value in 2000-2024 | `year` |
| 5 | Numeric range 1 <= population <= 2 billion | `population` |
| 6 | Valid date/time | `data_last_updated`, `collected_at` |
| 7 | Unique identifier | (`country_code`, `year`), first occurrence kept |
| 8 | Record structure (no missing/unexpected keys) | whole record |

**Handling invalid data:** invalid records are removed from the main stream and saved with the reason to `data/invalid_data.csv`; only `data/valid_data.csv` goes to cleaning.

**Validation test:** `validator.py` also builds 5 deliberately broken test records plus 1 control record, validates them with the same schema, and prints which were rejected and why. Results: `data/validation_test_results.csv`. Test records are never written to `valid_data.csv`.

Counters are saved to `data/consumer_report.json` and `data/validation_report.json`.

## Output files of Part 2
| File | Content |
|---|---|
| `data/consumed_data.json` | all records received from Kafka |
| `data/valid_data.csv` | valid records (input for `cleaner.py`) |
| `data/invalid_data.csv` | invalid records + `validation_errors` |
| `data/validation_test_results.csv` | results for deliberately invalid test records |
| `data/consumer_report.json`, `data/validation_report.json` | counters for the pipeline verification table |
