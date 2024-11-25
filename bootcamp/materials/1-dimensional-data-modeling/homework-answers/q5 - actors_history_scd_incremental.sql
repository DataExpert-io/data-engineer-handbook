
CREATE TYPE actor_scd_type AS (
    quality_class quality_class,
    is_active boolean,
    start_year INTEGER,
    end_year INTEGER
)

WITH last_year_scd AS (
    SELECT * FROM actors_history_scd
    WHERE end_year = 1975
), historical_scd AS (
    SELECT
        actorid,
        actor,
            quality_class,
            is_active,
            start_year,
            end_year
    FROM actors_history_scd
    WHERE end_year < 1975
), this_year_data AS (
    SELECT * FROM actors
    WHERE year = 1976
), unchanged_records AS (
    SELECT
        ty.actorid,
        ty.actor,
        ty.quality_class,
        ty.is_active,
        ly.start_year,
        ty.year AS end_year
    FROM this_year_data ty
        JOIN last_year_scd ly
            USING(actorid)
    WHERE ty.quality_class = ly.quality_class
        AND ty.is_active = ly.is_active
), changed_records AS (
    SELECT
        ty.actorid,
        ty.actor,
        UNNEST(ARRAY[
            ROW(
                ly.quality_class,
                ly.is_active,
                ly.start_year,
                ly.end_year

                )::actor_scd_type,
            ROW(
                ty.quality_class,
                ty.is_active,
                ty.year,
                ty.year
                )::actor_scd_type
        ]) as records
    FROM this_year_data AS ty
        LEFT JOIN last_year_scd AS ly
            USING(actorid)
    WHERE ty.quality_class <> ly.quality_class
        OR ty.is_active <> ly.is_active
), unnested_changed_records AS (
    SELECT
        actorid,
        actor,
        (records::actor_scd_type).quality_class,
        (records::actor_scd_type).is_active,
        (records::actor_scd_type).start_year,
        (records::actor_scd_type).end_year
    FROM changed_records
), new_records AS (
    SELECT
        ty.actorid,
        ty.actor,
        ty.quality_class,
        ty.is_active,
        ty.year AS start_year,
        ty.year AS end_year
    FROM this_year_data ty
        LEFT JOIN last_year_scd ly
            USING(actorid)
    WHERE ly.actorid IS NULL
)
SELECT
    *,
    1976 AS current_year
FROM (
    SELECT *
    FROM historical_scd

    UNION ALL

    SELECT *
    FROM unchanged_records

    UNION ALL

    SELECT *
    FROM unnested_changed_records

    UNION ALL

    SELECT *
    FROM new_records
) AS data