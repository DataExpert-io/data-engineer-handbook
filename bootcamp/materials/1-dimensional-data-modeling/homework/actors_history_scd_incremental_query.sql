-- SELECT * FROM actors_history_scd

-- Add new records where the condition is met
INSERT INTO actors_history_scd (actorid, films, quality_class, is_active, start_date, end_date, current_flag)
SELECT
    a.actorid,
    a.films,
    a.quality_class,
    a.is_active,
    CURRENT_TIMESTAMP AS start_date,
    NULL end_date,
    TRUE AS is_current
FROM actors a
LEFT JOIN actors_history_scd ah
ON ah.actorid = a.actorid AND current_flag = TRUE
WHERE (
    a.quality_class != ah.quality_class OR
    a.is_active != ah.is_active
) OR ah.actorid IS NULL


-- Expire the old records (we need to expire those after we passed the new ones)
UPDATE actors_history_scd
SET end_date = CURRENT_TIMESTAMP,
    current_flag = False
WHERE current_flag=True AND actorid in
(
    SELECT
        a.actorid
    FROM actors a
    INNER JOIN actors_history_scd ah
    ON (ah.actorid = a.actorid AND current_flag = True)
    WHERE (
        a.quality_class != ah.quality_class OR
        a.is_active != ah.is_active
    )
)

