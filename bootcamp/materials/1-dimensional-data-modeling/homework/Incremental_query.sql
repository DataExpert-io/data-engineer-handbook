WITH last_year_scd AS (
    SELECT * 
    FROM actors_history_scd
    WHERE end_date = 2007
),
historical_scd AS (
    SELECT
        actor,
        quality_class,
        is_active,
        start_date,
        end_date
    FROM actors_history_scd
    WHERE end_date < 2007
),
this_year_data AS (
    SELECT * 
    FROM actors 
    WHERE year = 2008
),
unchanged_records AS (
    SELECT
        t.actor,
        t.quality_class,
        t.is_active,
        l.start_date,
        t.year AS end_date
    FROM this_year_data t
    JOIN last_year_scd l ON l.actor = t.actor
    WHERE 
        COALESCE(t.quality_class, 'bad') = COALESCE(l.quality_class, 'bad') AND
        COALESCE(t.is_active, false) = COALESCE(l.is_active, false)
),
changed_records AS (
    SELECT
        t.actor,
        UNNEST(ARRAY[
            ROW(
                l.quality_class,
                l.is_active,
                l.start_date,
                l.end_date
            )::scd_type_class,
            ROW(
                t.quality_class,
                t.is_active,
                t.year,
                t.year
            )::scd_type_class
        ]) AS records
    FROM this_year_data t
    JOIN last_year_scd l ON l.actor = t.actor
    WHERE 
        COALESCE(t.quality_class, 'bad') <> COALESCE(l.quality_class, 'bad') OR
        COALESCE(t.is_active, false) <> COALESCE(l.is_active, false)
),
unnested_changed_records AS (
    SELECT
        actor,
        (records::scd_type_class).quality_class as quality_class,
        (records::scd_type_class).is_active as is_active,
        (records::scd_type_class).start_date as start_date,
        (records::scd_type_class).end_date as end_date
    FROM changed_records
),
new_records AS (
    SELECT
        t.actor,
        t.quality_class,
        t.is_active,
        t.year AS start_date,
        t.year AS end_date
    FROM this_year_data t
    LEFT JOIN last_year_scd l ON t.actor = l.actor
    WHERE l.actor IS NULL
)

SELECT *, 2008 AS current_year
FROM (
    SELECT * FROM historical_scd
    UNION ALL
    SELECT * FROM unchanged_records
    UNION ALL
    SELECT * FROM unnested_changed_records
    UNION ALL
    SELECT * FROM new_records
) final
;