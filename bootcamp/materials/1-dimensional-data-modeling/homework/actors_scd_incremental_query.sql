CREATE TYPE actor_scd_type AS (
    quality_class quality_class
    , is_active BOOLEAN
    , start_date INTEGER
    , end_date INTEGER
);

WITH last_year_scd AS (
    SELECT * FROM actors_history_scd 
    WHERE end_date = 1980
),
historical_scd AS (
    SELECT * FROM actors_history_scd 
    WHERE end_date < 1980
),
this_year_data AS (
    SELECT * FROM actors
    WHERE current_year = 1981
),
unchanged_records AS (
    SELECT 
        ts.actor_id
        , ts.actor_name 
        , ts.quality_class 
        , ts.is_active
        , ls.start_date
        , ts.current_year AS end_date
    FROM this_year_data ts 
    JOIN last_year_scd ls ON ls.actor_id = ts.actor_id
    WHERE ts.quality_class = ls.quality_class AND ts.is_active = ls.is_active
),
changed_records AS (
    SELECT 
        ts.actor_id
        , ts.actor_name
        , UNNEST(ARRAY[
                    ROW(
                        ls.quality_class
                        , ls.is_active
                        , ls.start_date
                        , ls.end_date
                        )::actor_scd_type
                    , ROW(
                        ts.quality_class
                        , ts.is_active
                        , ts.current_year
                        , ts.current_year
                        )::actor_scd_type
                ]) AS records
    FROM this_year_data ts 
    LEFT JOIN last_year_scd ls ON ls.actor_id = ts.actor_id
    WHERE (ts.quality_class <> ls.quality_class OR ts.is_active <> ls.is_active)
),
unnested_changed_records AS (
    SELECT
        actor_id
        , actor_name
        , (records::actor_scd_type).quality_class
        , (records::actor_scd_type).is_active
        , (records::actor_scd_type).start_date
        , (records::actor_scd_type).end_date
    FROM changed_records
),
new_records AS (
    SELECT 
        ts.actor_id 
        , ts.actor_name
        , ts.quality_class
        , ts.is_active
        , ts.current_year AS start_date
        , ts.current_year AS end_date
    FROM this_year_data ts
    LEFT JOIN last_year_scd ls ON ls.actor_id = ts.actor_id
    WHERE ls.actor_id IS NULL
)
SELECT * FROM historical_scd
UNION ALL
SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_records;