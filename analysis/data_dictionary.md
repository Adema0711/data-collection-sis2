# Data dictionary

One row represents one country and year. The key is `country_code` + `year`.

| Column | Type in the cleaned table | Meaning |
|---|---|---|
| country_code | Text | Three-letter country code |
| country_name | Text | Country name returned by the World Bank |
| region | Text | World Bank region |
| income_level | Text | World Bank income group in the source metadata |
| indicator_code | Text | World Bank indicator identifier, SP.POP.TOTL |
| data_last_updated | Date stored as text | Last update date reported by the API |
| collected_at | Timestamp stored as text | Time when the source records were collected |
| year | Integer | Observation year, 2000–2024 |
| population | Integer | Total population in people |
