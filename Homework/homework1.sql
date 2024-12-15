-- 1. Create a DDL table for Actors with the following fields:

-- SELECT MIN(year)
-- FROM actor_films; -- the min year is 1970, so we should start with the start = 1969 and move forward.
--
-- SELECT MAX(year)
-- FROM actor_films; -- the max year is 2021

-- a. films: An array of struct with the following fields - film, votes, rating, filmid (These are things that shouldn't change)
----- this wil be the films[] array

-- CREATE TYPE films_struct AS (
--     film TEXT
--     , votes INTEGER
--     , rating REAL
--     , filmid TEXT
--     );

-- b. quality_class: determined by avg rating of movies or most recent year

-- CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

-- going to denote the year of production as seasons for now

-- CASE
--     WHEN (seasons[CARDINALITY(seasons)]::films).rating > 8 THEN 'star'
--     WHEN (seasons[CARDINALITY(seasons)]::films).rating > 7 THEN 'good'
--     WHEN (seasons[CARDINALITY(seasons)]::films).rating > 6 THEN 'average'
--     ELSE 'bad'
-- END::quality_class

-- c. is_active: boolean field that indicates whether actor is still in the film industry (current year). Since we don't
-- care about the film, and we only care about the actor, we don't need to include the year in the struct, but we'll have
-- to be careful to look at the year that the actorid has a matching year row for the current_year of evaluation.

--  year = current_year AS is_active


-------DDL Table Creation
DROP TABLE actors;
CREATE TABLE actors (
    actor TEXT
    , actorid TEXT
    , current_year INTEGER
    , films_struct films_struct[]
    , quality_class quality_class
    , is_active BOOLEAN
    , PRIMARY KEY (actorid, current_year)
);


INSERT INTO actors
WITH last_year AS (
    SELECT *
    FROM actors
    WHERE current_year = 1969
),
this_year AS (
    SELECT *
    FROM actor_films
    WHERE year=1970
),
grouped_films AS (
    SELECT
        t.actor
        , t.actorid
        , t.year
        , ARRAY_AGG(ROW(
            t.film
            , t.votes
            , t.rating
            , t.filmid
        )::films_struct) AS new_films,
        AVG(t.rating) AS avg_rating
    FROM this_year t
    GROUP BY t.actor, t.actorid, t.year
)
SELECT
    COALESCE(g.actor, l.actor) AS actor
    , COALESCE(g.actorid, l.actorid) AS actorid
    , COALESCE(g.year, l.current_year + 1) as current_year
    , CASE
        WHEN l.films_struct IS NULL THEN g.new_films
        WHEN g.year IS NOT NULL THEN l.films_struct || g.new_films
        ELSE l.films_struct
        END AS films_struct
    , CASE
        WHEN g.year IS NOT NULL THEN
            CASE
                WHEN g.avg_rating > 8 THEN 'star'
                WHEN g.avg_rating > 7 THEN 'good'
                WHEN g.avg_rating > 6 THEN 'average'
                ELSE 'bad'
            END::quality_class
        ELSE l.quality_class
    END AS quality_class
    , CASE
        WHEN g.year IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS is_active
FROM grouped_films g
    FULL OUTER JOIN last_year l
        ON g.actorid = l.actorid;

-------------------------------------------
-- Creating a recursive function to add all years:


INSERT INTO actors
WITH RECURSIVE years AS (
    SELECT generate_series(1969, 2022) AS year
),
last_year AS (
    SELECT a.*
        , y.year AS current_processing_year
    FROM actors a
        CROSS JOIN years y
    WHERE a.current_year = y.year - 1
),
this_year AS (
    SELECT af.*
        , y.year AS current_processing_year
    FROM actor_films af
        CROSS JOIN years y
    WHERE af.year= y.year
),
grouped_films AS (
    SELECT
        t.actor
        , t.actorid
        , t.year
        , ARRAY_AGG(ROW(
            t.film
            , t.votes
            , t.rating
            , t.filmid
        )::films_struct) AS new_films,
        AVG(t.rating) AS avg_rating
    FROM this_year t
    GROUP BY t.actor, t.actorid, t.year
)
SELECT
    COALESCE(g.actor, l.actor) AS actor
    , COALESCE(g.actorid, l.actorid) AS actorid
    , COALESCE(g.year, l.current_year + 1) as current_year
    , CASE
        WHEN l.films_struct IS NULL THEN g.new_films
        WHEN g.year IS NOT NULL THEN l.films_struct || g.new_films
        ELSE l.films_struct
        END AS films_struct
    , CASE
        WHEN g.year IS NOT NULL THEN
            CASE
                WHEN g.avg_rating > 8 THEN 'star'
                WHEN g.avg_rating > 7 THEN 'good'
                WHEN g.avg_rating > 6 THEN 'average'
                ELSE 'bad'
            END::quality_class
        ELSE l.quality_class
    END AS quality_class
    , CASE
        WHEN g.year IS NOT NULL THEN TRUE
        ELSE FALSE
        END AS is_active
FROM grouped_films g
    FULL OUTER JOIN last_year l
        ON g.actorid = l.actorid;
--
-- SELECT *
-- FROM actors
-- WHERE actor='Gwyneth Paltrow'
-- ORDER BY current_year;


-- 3. DDL for actors history SCD, tracking type2 modeling with start/end date fields nad tracks quality and is active

-- DROP TABLE actors_history_scd;
CREATE TABLE actors_history_scd (
    actor TEXT
    , actorid TEXT
    , quality_class quality_class
    , is_active BOOLEAN
    , start_year INTEGER
    , end_year INTEGER
    , current_year INTEGER
    , PRIMARY KEY(actorid, start_year)
);


-- 4. Backfill query for actors_history_scd for single query


INSERT INTO actors_history_scd
WITH with_previous AS (
    SELECT actor
        , actorid
        , current_year
        , quality_class
        , is_active
        , LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_quality_class
        , LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year)     AS previous_is_active
    FROM actors
    WHERE current_year < 2021
),
with_indicators AS (
    SELECT *
        , CASE
            WHEN quality_class <> previous_quality_class THEN 1
            WHEN is_active <> previous_is_active THEN 1
            ELSE 0
        END AS change_indicator
    FROM with_previous
),
with_streaks AS (
    SELECT *
        , SUM(change_indicator) OVER (PARTITION BY actorid ORDER BY current_year) AS streak_identifier
    FROM with_indicators
)
SELECT
    actor
    , actorid
    , quality_class
    , is_active
    , MIN(current_year) AS start_year
    , MAX(current_year) AS end_year
    , 2020 AS current_year
FROM with_streaks
GROUP BY actor
        , actorid
        , quality_class
        , is_active
        , streak_identifier
ORDER BY actor
        , streak_identifier;



-- 5. Incremental query for 'actors_history_scd that combines prev year with new incoming data from actors table

CREATE TYPE scd_type AS (
    quality_class quality_class
    , is_active BOOLEAN
    , start_year INTEGER
    , end_year INTEGER
);


WITH last_year_scd AS (
    SELECT *
    FROM actors_history_scd
    WHERE current_year = 2020
    AND end_year = 2020
),
historical_scd AS (
    SELECT
        actor
        , actorid
        , quality_class
        , is_active
        , start_year
        , end_year
    FROM actors_history_scd
    WHERE current_year = 2020
        AND end_year < 2020
),
this_year_data AS (
    SELECT *
    FROM actors
    WHERE current_year=2021
),
unchanged_records AS (
    SELECT
        t.actor
    , t.actorid
    , t.quality_class
    , t.is_active
    , l.start_year
    , t.current_year AS end_year
    FROM this_year_data t
        JOIN last_year_scd l
        ON t.actorid = l.actorid
    WHERE t.quality_class = l.quality_class
        AND t.is_active = l.is_active
),
changed_records AS (
    SELECT
        t.actorid
        , t.actor
        , UNNEST(ARRAY[
            ROW(
                l.quality_class
                , l.is_active
                , l.start_year
                , l.end_year
                )::scd_type
            , ROW(
                t.quality_class
                , t.is_active
                , t.current_year
                , t.current_year
                )::scd_type
            ]) records
    FROM this_year_data t
        LEFT JOIN last_year_scd l
        ON t.actorid = l.actorid
    WHERE (t.quality_class <> l.quality_class)
        OR t.is_active <> l.is_active
),
unnested_changed_records AS (
    SELECT
        actor
        , actorid
        , (records::scd_type).quality_class
        , (records::scd_type).is_active
        , (records::scd_type).start_year
        , (records::scd_type).end_year
    FROM changed_records
),
new_records AS (
    SELECT
        t.actor
        , t.actorid
    , t.quality_class
    , t.is_active
    , t.current_year AS start_year
    , t.current_year AS end_year
    FROM this_year_data t
)
SELECT * FROM historical_scd

UNION ALL

SELECT * FROM unchanged_records

UNION ALL

SELECT * FROM unnested_changed_records

UNION ALL

SELECT * FROM new_records
