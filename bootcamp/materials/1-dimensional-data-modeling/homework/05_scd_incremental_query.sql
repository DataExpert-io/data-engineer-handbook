CREATE TYPE actor_scd_type AS (
    actor TEXT,
    quality_class quality_class_enum,
    is_active BOOLEAN,
    start_date DATE,
    end_date DATE
);


WITH process_date AS (
    SELECT CURRENT_DATE + INTERVAL '1 day' AS date
),

-- Active SCD records
last_scd AS (
    SELECT *
    FROM actors_history_scd, process_date
    WHERE end_date IS NULL
),

-- Current snapshot from actors table
this_snapshot AS (
    SELECT 
        actorid,
        actor,
        quality_class,
        is_active,
        CURRENT_DATE AS snapshot_date
    FROM actors
),

-- Expired records: Change the end_date for previous rows
expired_records AS (
    UPDATE actors_history_scd
    SET end_date = process_date.date - INTERVAL '1 day'
    FROM process_date, this_snapshot ts
    WHERE actors_history_scd.actorid = ts.actorid
      AND actors_history_scd.end_date IS NULL
      AND (
          ts.actor <> actors_history_scd.actor
       OR ts.quality_class <> actors_history_scd.quality_class
       OR ts.is_active <> actors_history_scd.is_active
      )
    RETURNING actors_history_scd.actorid, actors_history_scd.start_date
),

-- Changed actors: generate both expired and new rows
changed_records AS (
    SELECT
        ts.actorid,
        UNNEST(ARRAY[
            -- Expired old row
            ROW(
                ls.actor,
                ls.quality_class,
                ls.is_active,
                ls.start_date,
                process_date.date - INTERVAL '1 day'
            )::actor_scd_type,
            -- New current row
            ROW(
                ts.actor,
                ts.quality_class,
                ts.is_active,
                process_date.date,
                NULL
            )::actor_scd_type
        ]) AS records
    FROM this_snapshot ts
    JOIN last_scd ls ON ts.actorid = ls.actorid
    CROSS JOIN process_date
    WHERE ts.actor IS DISTINCT FROM ls.actor
       OR ts.quality_class IS DISTINCT FROM ls.quality_class
       OR ts.is_active IS DISTINCT FROM ls.is_active
),

unnested_changed_records AS (
    SELECT
        cr.actorid::TEXT AS actorid,
        (cr.records).actor::TEXT AS actor,
        (cr.records).quality_class::quality_class_enum AS quality_class,
        (cr.records).is_active::BOOLEAN AS is_active,
        (cr.records).start_date::DATE AS start_date,
        (cr.records).end_date::DATE AS end_date
    FROM changed_records cr
),

-- New actors that never appeared before
new_records AS (
    SELECT
        ts.actorid::TEXT AS actorid,
        ts.actor::TEXT AS actor,
        ts.quality_class::quality_class_enum AS quality_class,
        ts.is_active::BOOLEAN AS is_active,
        process_date.date::DATE AS start_date,
        NULL::DATE AS end_date
    FROM this_snapshot ts
    LEFT JOIN last_scd ls ON ts.actorid = ls.actorid
    CROSS JOIN process_date
    WHERE ls.actorid IS NULL
)

-- ✅ Perform the actual INSERT of new and updated rows, avoiding duplicates
INSERT INTO actors_history_scd (
    actorid, actor, quality_class, is_active, start_date, end_date
)
SELECT actorid, actor, quality_class, is_active, start_date, end_date
FROM (
    SELECT * FROM unnested_changed_records
    UNION ALL
    SELECT * FROM new_records
) AS to_insert
WHERE NOT EXISTS (
    SELECT 1
    FROM actors_history_scd
    WHERE actorid = to_insert.actorid
      AND start_date = to_insert.start_date
);
