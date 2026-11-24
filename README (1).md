# Part 1: API Collection and Kafka 

This part of the pipeline collects population data for 10 countries from the World Bank API, saves the raw data, and sends every record to a Kafka topic as JSON.


- Docker Desktop (running)

## API description

- **Source:** [World Bank Open Data](https://data.worldbank.org/indicator/SP.POP.TOTL), indicator `SP.POP.TOTL` (total population). The API is public and needs no API key. Documentation: https://datahelpdesk.worldbank.org/knowledgebase/articles/889392
- **Population endpoint:** `GET https://api.worldbank.org/v2/country/{country_codes}/indicator/SP.POP.TOTL`
- **Country metadata endpoint:** `GET https://api.worldbank.org/v2/country/{country_codes}`
- **Countries (10):** KAZ, USA, CHN, IND, BRA, RUS, DEU, JPN, NGA, EGY (separated by `;` in the URL)
- **Parameters:** `format=json`, `date=2000:2024` (year range), `per_page=500`, `page` (page number)
- **How data is collected:** `api_client.py` first requests country metadata (region and income level), then requests population values for all countries and years. Each record is built from both responses and gets two service fields, `data_last_updated` (from the API response) and `collected_at` (UTC time of collection). The raw data is saved to `data/raw_data.csv` and `data/raw_data.json` before any cleaning.
- **Error handling:** every request has a 30 s timeout and is retried up to 3 times if the network fails, the server returns a bad status, or the response has an unexpected format.
- **Dataset size:** 10 countries × 25 years = 250 records.

**Fields:**

| Field | Type | Description |
|---|---|---|
| `country_code` | categorical | ISO3 country code |
| `country_name` | categorical | Country name |
| `region` | categorical | World Bank region |
| `income_level` | categorical | World Bank income group |
| `year` | date/time | Year of the observation |
| `population` | numeric | Total population |
| `indicator_code` | categorical | Indicator code (`SP.POP.TOTL`) |
| `data_last_updated` | date/time | Date of the last update of the World Bank data |
| `collected_at` | date/time | When the record was collected |

## Quick start


```bash
python -m venv .venv
.venv\Scripts\activate           # Windows

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start the Kafka (docker should be running)
docker compose up -d

# 4. Collect data and publish it
cd src
python api_client.py            
python producer.py           



## Expected results
- `data/raw_data.csv` and `data/raw_data.json` contain 250 raw records.
- `api_client.py` prints `All records received: 250`.
- `producer.py` prints `Successfully published: 250` and `Submission errors: 0`.

## Checking the messages in Kafka

```bash
docker exec world_bank /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic worldbank_population
```

The output `worldbank_population:0:250` means partition 0 holds 250 messages.


