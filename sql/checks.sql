SELECT 'duplicate_keys', COUNT(*) FROM (
    SELECT country_code, year FROM population
    GROUP BY country_code, year
    HAVING COUNT(*) > 1
);

SELECT 'null_values', COUNT(*) FROM population
WHERE country_code IS NULL OR year IS NULL OR population IS NULL;

SELECT 'wrong_years', COUNT(*) FROM population
WHERE year < 2000 OR year > 2024;

SELECT 'wrong_population', COUNT(*) FROM population
WHERE population <= 0 OR population > 2000000000;

SELECT 'wrong_indicator', COUNT(*) FROM population
WHERE indicator_code != 'SP.POP.TOTL';

SELECT 'countries_without_25_years', COUNT(*) FROM (
    SELECT country_code FROM population
    GROUP BY country_code
    HAVING COUNT(*) != 25
);