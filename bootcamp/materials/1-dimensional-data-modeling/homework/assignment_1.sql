-- Assignment 1
CREATE TYPE film_struct AS (
    film TEXT,
    votes INT,
    rating REAL,
    filmid TEXT
);

CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

CREATE TABLE actors (
    actorid TEXT,
    actor TEXT,
    films film_struct[],
    quality_class quality_class,
    is_active BOOLEAN,
    current_year INT
);

-- Assignment 2
INSERT INTO actors (
    WITH last_ds AS (
            SELECT
                *
            FROM
                actors
            WHERE
                current_year = 1969
        ),
        films_compress AS (
            SELECT
                actorid,
                actor,
                ROW(
                    film,
                    votes,
                    rating,
                    filmid
                )::film_struct AS film_c,
                year
            FROM
                public.actor_films
            WHERE
                year = 1970
        ),
        ds AS (
            SELECT
                actorid,
                actor,
                ARRAY_AGG(film_c)::film_struct[] AS films,
                AVG((film_c::film_struct).rating) AS average_rating,
                year
            FROM
                films_compress
            GROUP BY
                actorid,
                actor,
                year
        )
        SELECT 
            COALESCE(l.actorid, t.actorid) as actorid,
            COALESCE(l.actor, t.actor) as actor,
            COALESCE(l.films, ARRAY[]::film_struct[]) || COALESCE(t.films, ARRAY[]::film_struct[]) AS films,
            CASE
                WHEN t.average_rating IS NOT NULL THEN
                (
                    CASE
                        WHEN average_rating > 8 THEN 'star'
                        WHEN average_rating > 7 AND average_rating <= 8 THEN 'good'
                        WHEN average_rating > 6 AND average_rating <= 7 THEN 'average'
                        ELSE 'bad'
                    END
                )::quality_class
                ELSE l.quality_class
            END AS quality_class,
            t.films IS NOT NULL AS is_active,
            1970 AS current_year
            FROM
                ds t
            FULL OUTER JOIN
                last_ds l ON t.actorid = l.actorid
)

-- Assignment 3
CREATE TABLE actors_history_scd (
    actorid TEXT,
    actor TEXT,
    quality_class quality_class,
    is_active BOOLEAN,
    start_year INT,
    end_year INT,
    current_year INT,
    PRIMARY KEY (actorid, start_year)
);

-- Assignment 4
INSERT INTO actors_history_scd (
    WITH with_previous AS (
        SELECT 
            actorid,
            actor,
            current_year,
            quality_class,
            is_active,
            LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_quality_class,
            LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_is_active
        FROM
            actors
        -- WHERE
        --     current_year < 1999
    ),
    with_indicators AS (
        SELECT
            *,
            CASE
                WHEN quality_class <> previous_quality_class THEN 1
                WHEN is_active <> previous_is_active
                THEN 1
                ELSE 0
            END AS change_ind
        FROM
            with_previous
    ),
    with_streaks AS (
        SELECT
            *,
            SUM(change_ind) OVER (PARTITION BY actorid ORDER BY current_year) AS streak_identifier
        FROM
            with_indicators
    )
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        MIN(current_year) AS start_year,
        MAX(current_year) AS end_year
        -- 1999 AS current_year
    FROM
        with_streaks
    GROUP BY actorid, actor, streak_identifier, quality_class, is_active
    ORDER BY actor
)

-- Assignment 5
CREATE TYPE scd_type AS (
    quality_class quality_class,
    is_active BOOLEAN,
    start_year INT,
    end_year INT
);

-- last year's data
WITH last_year_scd AS (
    SELECT * FROM actors_history_scd
    WHERE current_year = 1998
    AND end_year = 1998
),
-- data older than last year
historical_scd AS (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        start_year,
        end_year
    FROM actors_history_scd
    WHERE current_year = 1998
    AND end_year < 1998
),
-- latest records to be processed
this_year_data AS (
    SELECT * FROM actors
    WHERE current_year = 1999
),
-- unchanged records from latest data
unchanged_records AS (
    SELECT
        ty.actorid,
        ty.actor,
        ty.quality_class,
        ty.is_active,
        ly.start_year,
        ty.current_year AS end_year
    FROM this_year_data ty
    JOIN last_year_scd ly
    ON ly.actorid = ty.actorid
    WHERE ty.quality_class = ly.quality_class
        AND ty.is_active = ly.is_active
),
-- changed records from latest data
changed_records AS (
    SELECT
        ty.actorid,
        ty.actor,
        UNNEST(ARRAY[
            ROW(
                ly.quality_class,
                ly.is_active,
                ly.start_year,
                ly.end_year
                )::scd_type,
            ROW(
                ty.quality_class,
                ty.is_active,
                ty.current_year,
                ty.current_year
                )::scd_type
        ]) as records
    FROM this_year_data ty
    LEFT JOIN last_year_scd ly
    ON ly.actorid = ty.actorid
    WHERE (ty.quality_class <> ly.quality_class
        OR ty.is_active <> ly.is_active)
),
-- unnested changed records
unnested_changed_records AS (
    SELECT 
        actorid,
        actor,
        (records::scd_type).quality_class,
        (records::scd_type).is_active,
        (records::scd_type).start_year,
        (records::scd_type).end_year
    FROM changed_records
),
-- new records from latest data
new_records AS (
    SELECT
        ty.actorid,
        ty.actor,
        ty.quality_class,
        ty.is_active,
        ty.current_year AS start_year,
        ty.current_year AS end_year
    FROM this_year_data ty
    LEFT JOIN last_year_scd ly
    ON ty.actorid = ly.actorid
    WHERE ly.actorid IS NULL

)
SELECT 
    *,
    1999 AS current_year 
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
)


-- function to load data cumulatively
CREATE OR REPLACE FUNCTION load_data_for_years()
RETURNS void AS $$
DECLARE
    year INTEGER;
BEGIN
    FOR year IN 2001..2010 LOOP
        -- Replace this with your actual logic. For example:
        INSERT INTO your_target_table (col1, col2, year)
        SELECT col1, col2, year
        FROM your_source_table
        WHERE EXTRACT(YEAR FROM some_date_column) = year;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT load_data_for_years();