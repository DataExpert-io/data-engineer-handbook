-- Task 4: Backfill query for actors_history_scd
-- Write a "backfill" query that can populate the entire actors_history_scd table in a single query
-- Uses window functions to detect changes and collapse consecutive unchanged periods into single records

INSERT INTO actors_history_scd
WITH with_previous AS (SELECT actorid,
                              current_year,
                              quality_class,
                              is_active,
                              LAG(quality_class, 1)
                              OVER (PARTITION BY actorid ORDER BY current_year)                   AS previous_quality_class,
                              LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_is_active
                       FROM actors
                       WHERE current_year <= 2020),

     with_indicators AS (SELECT *,
                                CASE
                                    WHEN quality_class <> previous_quality_class THEN 1
                                    WHEN is_active <> previous_is_active THEN 1
                                    ELSE 0
                                    END AS change_indicator
                         FROM with_previous),

     with_streaks AS (SELECT *,
                             SUM(change_indicator)
                             OVER (PARTITION BY actorid ORDER BY current_year) AS streak_identifier
                      FROM with_indicators)

SELECT actorid,
       quality_class,
       is_active,
       MIN(current_year) AS start_date,
       MAX(current_year) AS end_date,
       2020              AS current_year
FROM with_streaks
GROUP BY actorid, streak_identifier, is_active, quality_class
ORDER BY actorid, streak_identifier;
