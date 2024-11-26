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

--
-- INSERT INTO actors
-- WITH last_year AS (
--     SELECT *
--     FROM actors
--     WHERE current_year = 1969
-- ),
-- this_year AS (
--     SELECT *
--     FROM actor_films
--     WHERE year=1970
-- ),
-- grouped_films AS (
--     SELECT
--         t.actor
--         , t.actorid
--         , t.year
--         , ARRAY_AGG(ROW(
--             t.film
--             , t.votes
--             , t.rating
--             , t.filmid
--         )::films_struct) AS new_films,
--         AVG(t.rating) AS avg_rating
--     FROM this_year t
--     GROUP BY t.actor, t.actorid, t.year
-- )
-- SELECT
--     COALESCE(g.actor, l.actor) AS actor
--     , COALESCE(g.actorid, l.actorid) AS actorid
--     , COALESCE(g.year, l.current_year + 1) as current_year
--     , CASE
--         WHEN l.films_struct IS NULL THEN g.new_films
--         WHEN g.year IS NOT NULL THEN l.films_struct || g.new_films
--         ELSE l.films_struct
--         END AS films_struct
--     , CASE
--         WHEN g.year IS NOT NULL THEN
--             CASE
--                 WHEN g.avg_rating > 8 THEN 'star'
--                 WHEN g.avg_rating > 7 THEN 'good'
--                 WHEN g.avg_rating > 6 THEN 'average'
--                 ELSE 'bad'
--             END::quality_class
--         ELSE l.quality_class
--     END AS quality_class
--     , CASE
--         WHEN g.year IS NOT NULL THEN TRUE
--         ELSE FALSE
--         END AS is_active
-- FROM grouped_films g
--     FULL OUTER JOIN last_year l
--         ON g.actorid = l.actorid;

-------------------------------------------
-- Creating a recursive function to add all years:

--
-- INSERT INTO actors
-- WITH RECURSIVE years AS (
--     SELECT generate_series(1969, 2022) AS year
-- ),
-- last_year AS (
--     SELECT a.*
--         , y.year AS current_processing_year
--     FROM actors a
--         CROSS JOIN years y
--     WHERE a.current_year = y.year - 1
-- ),
-- this_year AS (
--     SELECT af.*
--         , y.year AS current_processing_year
--     FROM actor_films af
--         CROSS JOIN years y
--     WHERE af.year= y.year
-- ),
-- grouped_films AS (
--     SELECT
--         t.actor
--         , t.actorid
--         , t.year
--         , ARRAY_AGG(ROW(
--             t.film
--             , t.votes
--             , t.rating
--             , t.filmid
--         )::films_struct) AS new_films,
--         AVG(t.rating) AS avg_rating
--     FROM this_year t
--     GROUP BY t.actor, t.actorid, t.year
-- )
-- SELECT
--     COALESCE(g.actor, l.actor) AS actor
--     , COALESCE(g.actorid, l.actorid) AS actorid
--     , COALESCE(g.year, l.current_year + 1) as current_year
--     , CASE
--         WHEN l.films_struct IS NULL THEN g.new_films
--         WHEN g.year IS NOT NULL THEN l.films_struct || g.new_films
--         ELSE l.films_struct
--         END AS films_struct
--     , CASE
--         WHEN g.year IS NOT NULL THEN
--             CASE
--                 WHEN g.avg_rating > 8 THEN 'star'
--                 WHEN g.avg_rating > 7 THEN 'good'
--                 WHEN g.avg_rating > 6 THEN 'average'
--                 ELSE 'bad'
--             END::quality_class
--         ELSE l.quality_class
--     END AS quality_class
--     , CASE
--         WHEN g.year IS NOT NULL THEN TRUE
--         ELSE FALSE
--         END AS is_active
-- FROM grouped_films g
--     FULL OUTER JOIN last_year l
--         ON g.actorid = l.actorid;

SELECT *
FROM actors
WHERE actor='Gwyneth Paltrow'
ORDER BY current_year;


-- 3. DDL for actors histrory SCD, tracking type2 modeling with start/end date fields nad tracks quality and is active

CREATE TABLE actors_history_scd (

)


