# Pipeline verification

| Stage | In | Out | Difference | Comment |
|---|---|---|---|---|
| API -> Kafka producer | 250 | 250 | 0 | OK |
| producer -> topic | 250 | 250 | 0 | OK |
| topic -> consumer | 250 | 250 | 0 | OK |
| consumer -> validation | 250 | 250 | 0 | OK |
| validation | 250 | 250 | 0 | OK |
| valid -> cleaning | 250 | 250 | 0 | OK |
| cleaning | 250 | 250 | 0 | OK |
| cleaning -> database | 250 | 250 | 0 | OK |
| second load | 250 | 250 | 0 | OK |

| SQL check | Result |
|---|---|
| duplicate_keys | 0 |
| null_values | 0 |
| wrong_years | 0 |
| wrong_population | 0 |
| wrong_indicator | 0 |
| countries_without_25_years | 0 |
