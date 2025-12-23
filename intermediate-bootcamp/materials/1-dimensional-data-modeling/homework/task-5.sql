-- Task 5: Incremental query for actors_history_scd
-- Write an "incremental" query that combines the previous year's SCD data with new incoming data from the actors table
-- Handles four scenarios: historical records, unchanged records, changed records, and new records

INSERT INTO actors_history_scd
WITH last_year_scd AS (SELECT *
                       FROM actors_history_scd
                       WHERE current_year = 2020
                         AND end_date = 2020),

     historical_scd AS (SELECT actorid,
                               quality_class,
                               is_active,
                               start_date,
                               end_date
                        FROM actors_history_scd
                        WHERE current_year = 2020
                          AND end_date < 2020),

     this_year_data AS (SELECT *
                        FROM actors
                        WHERE current_year = 2021),

     unchanged_records AS (SELECT ts.actorid,
                                  ts.quality_class,
                                  ts.is_active,
                                  ls.start_date,
                                  ts.current_year AS end_date
                           FROM this_year_data ts
                                    JOIN last_year_scd ls ON ts.actorid = ls.actorid
                           WHERE ts.quality_class = ls.quality_class
                             AND ts.is_active = ls.is_active),

     changed_records AS (SELECT ts.actorid,
                                UNNEST(ARRAY [
                                    ROW (ls.quality_class, ls.is_active, ls.start_date, ls.end_date)::actors_scd_type,
                                    ROW (ts.quality_class, ts.is_active, ts.current_year, ts.current_year)::actors_scd_type
                                    ]) AS records
                         FROM this_year_data ts
                                  LEFT JOIN last_year_scd ls ON ts.actorid = ls.actorid
                         WHERE ts.quality_class <> ls.quality_class
                            OR ts.is_active <> ls.is_active),

     unnested_changed_records AS (SELECT actorid,
                                         (records::actors_scd_type).quality_class,
                                         (records::actors_scd_type).is_active,
                                         (records::actors_scd_type).start_date,
                                         (records::actors_scd_type).end_date
                                  FROM changed_records),

     new_records AS (SELECT ts.actorid,
                            ts.quality_class,
                            ts.is_active,
                            ts.current_year AS start_date,
                            ts.current_year AS end_date
                     FROM this_year_data ts
                              LEFT JOIN last_year_scd ls ON ts.actorid = ls.actorid
                     WHERE ls.actorid IS NULL),

     all_records AS (SELECT *, 2021 AS current_year
                     FROM (SELECT *
                           FROM historical_scd
                           UNION ALL
                           SELECT *
                           FROM unchanged_records
                           UNION ALL
                           SELECT *
                           FROM unnested_changed_records
                           UNION ALL
                           SELECT *
                           FROM new_records) a)

SELECT *
FROM all_records
ON CONFLICT (actorid, start_date, current_year) DO NOTHING;
