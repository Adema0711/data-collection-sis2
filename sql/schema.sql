CREATE TABLE IF NOT EXISTS population (
    country_code TEXT NOT NULL,
    country_name TEXT NOT NULL,
    region TEXT NOT NULL,
    income_level TEXT NOT NULL,
    year INTEGER NOT NULL,
    population INTEGER NOT NULL,
    indicator_code TEXT NOT NULL,
    data_last_updated TEXT NOT NULL,
    collected_at TEXT NOT NULL,
    PRIMARY KEY (country_code, year)
);