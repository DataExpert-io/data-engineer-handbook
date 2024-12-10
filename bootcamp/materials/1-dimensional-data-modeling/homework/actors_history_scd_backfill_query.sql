-- Original insterting of the data
INSERT INTO actors_history_scd (actorid, films, quality_class, is_active, start_date, end_date, current_flag)
SELECT
    actorid,
    films,
    quality_class,
    is_active,
    CURRENT_TIMESTAMP AS start_date,
    NULL end_date,
    TRUE AS is_current
FROM actors
