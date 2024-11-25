INSERT INTO actors_history_scd
WITH streak_start AS (
    SELECT
        actorid,
        actor,
            quality_class,
            is_active,
            year,
        (LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY year) <> quality_class
            OR LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY year) IS NULL)
        AND (LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY year) <> is_active
            OR LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY year) IS NULL) AS did_change
    FROM actors
), streak_identified AS (
    SELECT
        actorid,
        actor,
            quality_class,
            is_active,
            year,
        SUM(CASE WHEN did_change THEN 1 ELSE 0 END) OVER(PARTITION BY actorid ORDER BY year) AS streak_identifier
    FROM streak_start
), aggregated AS (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        streak_identifier,
        MIN(year) AS start_year,
        MAX(year) AS end_year
    FROM streak_identified
    GROUP BY 1, 2, 3, 4, 5
)
SELECT actorid, actor, quality_class, is_active, start_year, end_year, end_year
FROM aggregated;