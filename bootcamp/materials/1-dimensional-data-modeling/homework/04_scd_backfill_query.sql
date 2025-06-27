INSERT INTO actors_history_scd (
    actorid,
    actor,
    quality_class,
    is_active,
    start_date,
    end_date
)
SELECT
    actorid,
    actor,
    quality_class,
    is_active,
    CURRENT_DATE AS start_date,
    NULL AS end_date
FROM actors;
