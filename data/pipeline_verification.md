# Pipeline verification

| Stage | In | Out | Difference | Comment |
|---|---|---|---|---|
| API -> Kafka producer | 250 | - | - | stage was not run |
| producer -> topic | - | - | - | stage was not run |
| topic -> consumer | - | - | - | stage was not run |
| consumer -> validation | - | - | - | stage was not run |
| validation | - | - | - | stage was not run |
| valid -> cleaning | - | 250 | - | stage was not run |
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
