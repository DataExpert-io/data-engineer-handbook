select * from actor_films limit 10;
select min(year), max(year) from actor_films;


-- 1. DDL for actors table

DROP TYPE films CASCADE;
CREATE TYPE films AS (
  film TEXT,
  votes INTEGER,
  rating REAL,
  film_id TEXT
);

CREATE TYPE quality_class AS ENUM (
  'star', 'good', 'average', 'bad'
);

DROP TABLE IF EXISTS actors;
CREATE TABLE actors (
  actor_id TEXT,
  actor_name TEXT,
  films films[],
  quality_class quality_class,
  is_active BOOLEAN,
  current_year INTEGER,
  PRIMARY KEY (actor_id, current_year)
);
SELECT * FROM actors;

-- 2. Cumulative table generation query

-- tests

SELECT * FROM actor_films WHERE actorid = 'nm0000003';

SELECT
  actor, array_agg(row(film, votes, rating, filmid)) as films
-- ,
-- actor, film, year, votes, rating, filmid
FROM actor_films WHERE actorid = 'nm0000003' and year = 1970
GROUP BY actor;

SELECT
  actor, array_agg(row(film, votes, rating, filmid)::films) as films,
  AVG(rating) AS avg_rating,
  CASE
    WHEN AVG(rating) > 8 THEN 'star'
    WHEN AVG(rating) > 7 THEN 'good'
    WHEN AVG(rating) > 6 THEN 'average'
    ELSE 'bad'
    END::quality_class AS quality_class
-- ,
-- actor, film, year, votes, rating, filmid
FROM actor_films WHERE actorid = 'nm0000003' and year < 1972
GROUP BY actor;

WITH last_year AS (
  SELECT
    'last_year' as timeframe,
    actor, array_agg(row(film, votes, rating, filmid)::films) as films,
    AVG(rating) AS avg_rating, TRUE AS is_active
  FROM actor_films WHERE actorid = 'nm0000003' AND year = 1970
  GROUP BY actor
),
  this_year AS (
SELECT
  'this_year' as timeframe,
  actor, array_agg(row(film, votes, rating, filmid)::films) as films,
  AVG(rating) AS avg_rating, TRUE AS is_active
FROM actor_films WHERE actorid = 'nm0000003' AND year = 1971
GROUP BY actor
)
SELECT * FROM last_year UNION SELECT * FROM this_year
-- UNION
-- SELECT 'joined_year' as timeframe,
--   COALESCE(ty.actor, ly.actor) AS actor,
--        ly.films || ty.films AS films,
--        (ty.avg_rating + ly.avg_rating)/2 AS avg_rating, TRUE AS is_active
-- FROM this_year ty JOIN last_year ly ON ly.actor = ty.actor
;

{("The Bear and the Doll",431,6.4,tt0064779),("Les novices",219,5.1,tt0066164),("Rum Runners",469,5.6,tt0066857),("Frenchie King",1054,5.3,tt0067637)}
5.600000023841858

WITH last_year AS (
  SELECT
    'last_year' as timeframe,
    actor, array_agg(row(film, votes, rating, filmid)::films) as films,
    AVG(rating) AS avg_rating, TRUE AS is_active
  FROM actor_films WHERE actorid = 'nm0000003' AND year = 1970
  GROUP BY actor
),
this_year AS (
 SELECT
   'this_year' as timeframe,
   actor, array_agg(row(film, votes, rating, filmid)::films) as films,
   AVG(rating) AS avg_rating, TRUE AS is_active
 FROM actor_films WHERE actorid = 'nm0000003' AND year = 1971
 GROUP BY actor
)
SELECT
-- AVG((
           UNNEST(ly.films || ty.films)
--            ).rating)
FROM this_year ty JOIN last_year ly ON ly.actor = ty.actor;
-- SELECT AVG((UNNEST(ly.films || ty.films)).rating) FROM this_year ty JOIN last_year ly ON ly.actor = ty.actor)
-- didn't work
, consolidated AS (
SELECT UNNEST(ly.films || ty.films) films_unnested
FROM this_year ty JOIN last_year ly ON ly.actor = ty.actor
)
SELECT AVG((films_unnested).rating) FROM consolidated;

-- test json use
WITH last_year AS (
  SELECT
    'last_year' as timeframe,
    actor, array_agg(json_build_object('film', film,
                                       'votes', votes,
                                       'rating', rating,
                                       'filmid', filmid)) as films,
    AVG(rating) AS avg_rating, TRUE AS is_active
  FROM actor_films WHERE actorid = 'nm0000003' AND year = 1970
  GROUP BY actor
)
select * from last_year;

WITH last_year AS (
--   SELECT actor_id, actor_name,
--          films, is_active, current_year
--   FROM actors WHERE current_year = 1970
  SELECT actorid as actor_id, actor as actor_name,
         array_agg(row(film, votes, rating, filmid)::films) as films
  FROM actor_films WHERE year = 1970
  GROUP BY actorid, actor
),
this_year AS (
 SELECT actorid, actor,
        array_agg(row(film, votes, rating, filmid)::films) as films,
        TRUE AS is_active
 FROM actor_films WHERE year = 1972
 GROUP BY actorid, actor
),
unnested_years AS (
 SELECT
   COALESCE(ty.actorid, ly.actor_id) AS actor_id,
   COALESCE(ty.actor, ly.actor_name) AS actor_name,
   ly.films || ty.films AS films,
   COALESCE(ty.is_active, FALSE) AS is_active,
   UNNEST(ly.films || ty.films) films_unnested
 FROM this_year ty FULL JOIN last_year ly ON ly.actor_id = ty.actorid
)
select * from unnested_years;

-- ALMOST THERE TEST

WITH last_year AS (
  SELECT actor_id, actor_name,
         films, current_year
  FROM actors WHERE current_year = 1970
),
     this_year AS (
       SELECT actorid, actor,
              array_agg(row(film, votes, rating, filmid)::films) as films,
              TRUE AS is_active, year
       FROM actor_films WHERE year = 1970
       GROUP BY actorid, actor, year
     ),
     unnested_years AS (
       SELECT
         COALESCE(ty.actorid, ly.actor_id) AS actor_id,
         COALESCE(ty.actor, ly.actor_name) AS actor_name,
         ly.films || ty.films AS films,
         UNNEST(ly.films || ty.films) AS films_unnested,
         COALESCE(ty.is_active, FALSE) AS is_active,
         ty.year AS current_year
       FROM this_year ty FULL JOIN last_year ly ON ly.actor_id = ty.actorid
     ),
     consolidated_year_rating AS (
       SELECT actor_id, actor_name, films,
              AVG((films_unnested).rating) AS avg_rating,
              is_active, current_year
       FROM unnested_years
       GROUP BY actor_id, actor_name, films, is_active, current_year
     )
SELECT actor_id, actor_name, films,
       CASE WHEN is_active IS TRUE THEN
              CASE
                WHEN AVG(avg_rating) > 8 THEN 'star'
                WHEN AVG(avg_rating) > 7 THEN 'good'
                WHEN AVG(avg_rating) > 6 THEN 'average'
                ELSE 'bad'
                END::quality_class AS quality_class,
       CASE WHEN ty.actorid IS NOT NULL THEN 1 ELSE 0 END AS is_active
FROM consolidated_year_rating;



-- QUERY VALIDATION
SELECT * FROM actors ORDER BY current_year DESC, actor_id;
SELECT * FROM actors WHERE current_year = 1972 ORDER BY actor_id;
SELECT a2.quality_class, a1.* FROM actors a1 JOIN actors a2 ON
  a1.actor_id = a2.actor_id AND a2.current_year = 1970 AND
  a1.current_year = 1971 AND a1.quality_class != a2.quality_class;
SELECT * FROM actors WHERE is_active IS FALSE;
select * from actors where actor_id = 'nm0000977';

-- QUERY
TRUNCATE TABLE actors;

SELECT * FROM actors ORDER BY current_year DESC, actor_id;

-- 2. v2
-- quality_class: This field represents an actor's performance quality,
-- determined by the average rating of movies of their most recent year.
-- Therefore, consider only the most recent year

-- QUERY VALIDATION
SELECT * FROM actors WHERE current_year = 1972 ORDER BY actor_id;
SELECT a2.quality_class, a1.* FROM actors a1 JOIN actors a2 ON
  a1.actor_id = a2.actor_id AND a2.current_year = 1970 AND
  a1.current_year = 1971 AND a1.quality_class != a2.quality_class;
SELECT * FROM actors WHERE is_active IS FALSE;
select * from actors where actor_id = 'nm0000977';
INSERT INTO actors
WITH last_year AS (
  SELECT actor_id, actor_name,
         films, quality_class, current_year
  FROM actors WHERE current_year = 1974
),
     this_year AS (
       SELECT actorid, actor,
              array_agg(row(film, votes, rating, filmid)::films) as films,
              TRUE AS is_active, year
       FROM actor_films WHERE year = 1975
       GROUP BY actorid, actor, year
     ),
     consolidated_years AS (
       SELECT
         COALESCE(ty.actorid, ly.actor_id) AS actor_id,
         COALESCE(ty.actor, ly.actor_name) AS actor_name,
         ly.films || ty.films AS films,
         ly.quality_class AS quality_class,
         COALESCE(ty.is_active, FALSE) AS is_active,
         COALESCE(ty.year, ly.current_year + 1) AS current_year
       FROM this_year ty FULL JOIN last_year ly ON ly.actor_id = ty.actorid
     ),
     unnested_films AS (
       SELECT actor_id, actor_name, films, quality_class, is_active, current_year,
              UNNEST(films) AS films_unnested
       FROM consolidated_years
       WHERE is_active IS TRUE
     ),
     consolidated_year_rating AS (
       SELECT actor_id, actor_name, films,
              AVG((films_unnested).rating) AS avg_rating,
              is_active, current_year
       FROM unnested_films
       GROUP BY actor_id, actor_name, films, is_active, current_year
     )
SELECT
  cy.actor_id,
  cy.actor_name,
  cy.films,
  CASE WHEN cy.is_active IS TRUE THEN
         CASE
           WHEN AVG(cyr.avg_rating) > 8 THEN 'star'
           WHEN AVG(cyr.avg_rating) > 7 THEN 'good'
           WHEN AVG(cyr.avg_rating) > 6 THEN 'average'
           ELSE 'bad'
           END::quality_class
       ELSE cy.quality_class END AS quality_class,
  cy.is_active,
  cy.current_year
FROM consolidated_years cy
       FULL JOIN consolidated_year_rating cyr ON cy.actor_id = cyr.actor_id
GROUP BY cy.actor_id, cy.actor_name, cy.films, cy.quality_class,
         cy.is_active, cy.current_year;

-- QUERY

TRUNCATE TABLE actors;

INSERT INTO actors
WITH last_year AS (
  SELECT actor_id, actor_name, films, quality_class, current_year
  FROM actors WHERE current_year = 1974
),
this_year AS (
 SELECT actorid, actor,
        ARRAY_AGG(row(film, votes, rating, filmid)::films) as films,
        AVG(rating) AS avg_rating, TRUE AS is_active, year
 FROM actor_films WHERE year = 1975
 GROUP BY actorid, actor, year
)
SELECT
  COALESCE(ty.actorid, ly.actor_id) AS actor_id,
  COALESCE(ty.actor, ly.actor_name) AS actor_name,
  ly.films || ty.films AS films,
  CASE WHEN ty.avg_rating IS NOT NULL THEN
    CASE
      WHEN ty.avg_rating > 8 THEN 'star'
      WHEN ty.avg_rating > 7 THEN 'good'
      WHEN ty.avg_rating > 6 THEN 'average'
      ELSE 'bad'
    END::quality_class
  ELSE ly.quality_class END AS quality_class,
  COALESCE(ty.is_active, FALSE) AS is_active,
  COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM this_year ty FULL JOIN last_year ly ON ly.actor_id = ty.actorid
;

SELECT * FROM actors WHERE actor_id = 'nm0000003';
SELECT min(current_year), max(current_year) FROM actors;
SELECT MAX(year) FROM actor_films;

-- FILL ALL actors TABLE

DO $$
DECLARE
  min_year INT := 1975;
  max_year INT := (SELECT MAX(year) FROM actor_films);
BEGIN
  FOR i IN min_year..max_year LOOP
      RAISE NOTICE 'i = %', i;
    END LOOP;
END $$


DO $$
DECLARE
  min_year INT := 1975;
  max_year INT := (SELECT MAX(year) - 1 FROM actor_films);
BEGIN
  FOR curr_year IN min_year..max_year LOOP
      INSERT INTO actors
      WITH last_year AS (
        SELECT actor_id, actor_name, films, quality_class, current_year
        FROM actors WHERE current_year = curr_year
      ),
       this_year AS (
         SELECT actorid, actor,
                ARRAY_AGG(row(film, votes, rating, filmid)::films) as films,
                AVG(rating) AS avg_rating, TRUE AS is_active, year
         FROM actor_films WHERE year = curr_year + 1
         GROUP BY actorid, actor, year
       )
      SELECT
        COALESCE(ty.actorid, ly.actor_id) AS actor_id,
        COALESCE(ty.actor, ly.actor_name) AS actor_name,
        ly.films || ty.films AS films,
        CASE WHEN ty.avg_rating IS NOT NULL THEN
               CASE
                 WHEN ty.avg_rating > 8 THEN 'star'
                 WHEN ty.avg_rating > 7 THEN 'good'
                 WHEN ty.avg_rating > 6 THEN 'average'
                 ELSE 'bad'
                 END::quality_class
             ELSE ly.quality_class END AS quality_class,
        COALESCE(ty.is_active, FALSE) AS is_active,
        COALESCE(ty.year, ly.current_year + 1) AS current_year
      FROM this_year ty FULL JOIN last_year ly ON ly.actor_id = ty.actorid;
    END LOOP;
END $$

SELECT * FROM actors WHERE actor_id = 'nm0000003';
SELECT min(current_year), max(current_year) FROM actors;



-- 3. DDL for `actors_history_scd` table

DROP TABLE IF EXISTS actors_history_scd;
CREATE TABLE actors_history_scd (
  actor_id TEXT, actor_name TEXT, films films[], quality_class quality_class,
  is_active BOOLEAN, start_year INTEGER, end_year INTEGER,
  PRIMARY KEY (actor_id, start_year)
);

-- 4. Backfill query for `actors_history_scd`

SELECT * FROM GENERATE_SERIES((SELECT MIN(year) FROM actor_films), (SELECT MAX(year) FROM actor_films));

WITH movie_years AS (
  SELECT * FROM GENERATE_SERIES(
    (SELECT MIN(year) FROM actor_films), (SELECT MAX(year) - 1 FROM actor_films)
  ) AS years
),
first_movie_year AS (
  SELECT actorid, actor, MIN(year) first_year, MAX(year) last_year
  FROM actor_films GROUP BY actorid, actor
),
movie_and_years AS (
  SELECT * FROM first_movie_year fmy JOIN movie_years my ON fmy.first_year <= my.years
           AND fmy.last_year + 1 >= my.years
)
-- SELECT * FROM movie_years;
-- SELECT * FROM movie_and_years;
-- SELECT * FROM movie_and_years WHERE actorid = 'nm0000003';
, windowed_actors AS (
  SELECT DISTINCT may.actorid, may.actor, ARRAY_REMOVE( ARRAY_AGG(
    CASE WHEN af.year IS NOT NULL
    THEN ROW(af.film, af.votes, af.rating, af.filmid)::films
    END) OVER (PARTITION BY af.actorid, af.actor ORDER BY may.years)
    , NULL) AS films,
    CASE WHEN af.year IS NOT NULL THEN
    AVG(af.rating) OVER (PARTITION BY af.actorid, af.actor, af.year)
    ELSE NULL END avg_rating,
    CASE WHEN af.year IS NOT NULL THEN TRUE ELSE FALSE END AS is_active,
    may.years AS current_year,
    may.first_year, may.last_year
    FROM movie_and_years may
    LEFT JOIN actor_films af ON af.actorid = may.actorid AND af.year = may.years
    )
-- SELECT *, films = '{}' AS json_check FROM windowed_actors where actorid = 'nm0000003';
, windowed_actors_treated AS (
  SELECT actorid, actor, CASE WHEN films = '{}' THEN
    LAG(films, 1) OVER (PARTITION BY actorid, actor ORDER BY current_year)
    ELSE films END AS films,
    CASE
     WHEN avg_rating > 8 THEN 'star'
     WHEN avg_rating > 7 THEN 'good'
     WHEN avg_rating > 6 THEN 'average'
     ELSE 'bad'
    END::quality_class AS quality_class,
    CASE WHEN avg_rating IS NULL THEN
    LAG(avg_rating, 1) OVER (PARTITION BY actorid, actor ORDER BY current_year)
    ELSE avg_rating END AS avg_rating,
    is_active, current_year,
    LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
    LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
  FROM windowed_actors
)
SELECT * FROM windowed_actors_treated where actorid = 'nm0000003';


-- 2nd approach: mimic actors input

DROP TABLE IF EXISTS actors_history_scd;
CREATE TABLE actors_history_scd (
actor_id TEXT, actor_name TEXT, films films[], quality_class quality_class,
is_active BOOLEAN, start_date DATE, end_date DATE, current_year INTEGER,
PRIMARY KEY (actor_id, start_date));

TRUNCATE TABLE actors_history_scd;



-- 3rd approach - simulate what was done in class 1-2

WITH movie_years AS (
  SELECT * FROM GENERATE_SERIES(
                  (SELECT MIN(year) FROM actor_films), (SELECT MAX(year) FROM actor_films)
                ) AS years
),
first_movie_year AS (
 SELECT actorid, actor, MIN(year) first_year, MAX(year) last_year
 FROM actor_films GROUP BY actorid, actor
 ),
movie_and_years AS (
 SELECT * FROM first_movie_year fmy
   JOIN movie_years my ON fmy.first_year <= my.years
)
-- SELECT * FROM movie_years;
-- SELECT * FROM movie_and_years;
-- SELECT * FROM movie_and_years WHERE actorid = 'nm0000003';
, avg_rating AS (
  SELECT actorid, actor, year, AVG(rating) avg_rating
  FROM actor_films GROUP BY actorid, actor, year
)
, windowed_actors AS (
  SELECT DISTINCT may.actorid, may.actor,
  CASE WHEN ar.avg_rating IS NOT NULL THEN CASE
    WHEN ar.avg_rating > 8 THEN 'star'
    WHEN ar.avg_rating > 7 THEN 'good'
    WHEN ar.avg_rating > 6 THEN 'average'
    ELSE 'bad'
  END::quality_class ELSE NULL::quality_class END AS quality_class,
  CASE WHEN af.year IS NOT NULL THEN TRUE ELSE FALSE END AS is_active,
  may.years AS current_year,
  may.first_year, may.last_year
  FROM movie_and_years may
  LEFT JOIN actor_films af ON af.actorid = may.actorid AND af.year = may.years
  LEFT JOIN avg_rating ar ON ar.actorid = may.actorid AND ar.year = may.years
)
, windowed_quality_class AS (
  SELECT actorid, actor, CASE WHEN quality_class IS NULL THEN 'star'
--     LAG(quality_class, 1) OVER (PARTITION BY actorid, actor ORDER BY current_year)
    ELSE quality_class END AS quality_class,
  is_active, current_year, first_year, last_year
  FROM windowed_actors
)
SELECT * FROM windowed_actors where actorid = 'nm0000001';
,
         CASE WHEN avg_rating IS NULL THEN
                  LAG(avg_rating, 1) OVER (PARTITION BY actorid, actor ORDER BY current_year)
              ELSE avg_rating END AS avg_rating,
         is_active, current_year,
         LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
         LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
  FROM windowed_actors
)
SELECT * FROM windowed_actors_treated where actorid = 'nm0000003';

-- 4th tentative - class 1-1 interclass 2 + class 1-2

WITH movie_years AS (
SELECT * FROM GENERATE_SERIES(
(SELECT MIN(year) FROM actor_films), (SELECT MAX(year) - 1 FROM actor_films)
) AS years
),
first_movie_year AS (
SELECT actorid, actor, MIN(year) first_year, MAX(year) last_year
FROM actor_films GROUP BY actorid, actor
),
movie_and_years AS (
SELECT * FROM first_movie_year fmy JOIN movie_years my ON fmy.first_year <= my.years
)
-- SELECT * FROM movie_years;
-- SELECT * FROM movie_and_years;
-- SELECT * FROM movie_and_years WHERE actorid = 'nm0000003';
, windowed_actors AS (
SELECT DISTINCT may.actorid, may.actor, ARRAY_REMOVE( ARRAY_AGG(
CASE WHEN af.year IS NOT NULL
 THEN ROW(af.film, af.votes, af.rating, af.filmid)::films
END) OVER (PARTITION BY may.actorid, may.actor ORDER BY may.years)
, NULL) AS films,
-- CASE WHEN af.year IS NOT NULL THEN
-- AVG(af.rating) OVER (PARTITION BY af.actorid, af.actor, af.year)
-- ELSE NULL END avg_rating,
CASE WHEN af.year IS NOT NULL THEN TRUE ELSE FALSE END AS is_active,
may.years AS current_year,
may.first_year, may.last_year
FROM movie_and_years may
LEFT JOIN actor_films af ON af.actorid = may.actorid AND af.year = may.years
)
-- SELECT * FROM windowed_actors where actorid = 'nm0000003' ORDER BY current_year;
SELECT
actorid, actor, films,
avg((unnest(films)).rating),
CARDINALITY(films),
films[CARDINALITY(films)]
FROM windowed_actors
where actorid = 'nm0000003' ORDER BY current_year;

-- 5th tentative

WITH movie_years AS (
SELECT * FROM GENERATE_SERIES(
(SELECT MIN(year) FROM actor_films), (SELECT MAX(year) FROM actor_films)
) AS years
),
first_movie_year AS (
SELECT actorid, actor, MIN(year) first_year, MAX(year) last_year
FROM actor_films GROUP BY actorid, actor
),
movie_and_years AS (
SELECT * FROM first_movie_year fmy
JOIN movie_years my ON fmy.first_year <= my.years
)
-- SELECT * FROM movie_years;
-- SELECT * FROM movie_and_years;
-- SELECT * FROM movie_and_years WHERE actorid = 'nm0000003';
, avg_rating AS (
SELECT actorid, actor, year, AVG(rating) avg_rating
FROM actor_films GROUP BY actorid, actor, year
)
, windowed_actors AS (
SELECT DISTINCT may.actorid, may.actor, ARRAY_REMOVE( ARRAY_AGG(
CASE WHEN ar.year IS NOT NULL
THEN ar.avg_rating END) OVER (PARTITION BY may.actorid, may.actor ORDER BY may.years)
, NULL) AS avg_rating_array,
CASE WHEN ar.year IS NOT NULL THEN TRUE ELSE FALSE END AS is_active,
may.years AS current_year,
may.first_year, may.last_year
FROM movie_and_years may
LEFT JOIN avg_rating ar ON ar.actorid = may.actorid AND ar.year = may.years
)
, actors_scd_base AS (
SELECT actorid, actor, CASE
  WHEN avg_rating_array[CARDINALITY(avg_rating_array)] > 8 THEN 'star'
  WHEN avg_rating_array[CARDINALITY(avg_rating_array)] > 7 THEN 'good'
  WHEN avg_rating_array[CARDINALITY(avg_rating_array)] > 6 THEN 'average'
  ELSE 'bad'
  END::quality_class AS quality_class, is_active, current_year
FROM windowed_actors
)
-- SELECT * FROM actors_scd_base where actorid = 'nm0000001' ORDER BY current_year;
, actors_scd_previous AS (
  SELECT *,
    LAG(quality_class, 1) OVER (PARTITION BY actorid, actor ORDER BY current_year) AS prev_quality_class,
    LAG(is_active, 1) OVER (PARTITION BY actorid, actor ORDER BY current_year) AS prev_is_active
  FROM actors_scd_base
)
-- SELECT * FROM actors_scd_previous where actorid = 'nm0000001' ORDER BY current_year;
, actors_scd_indicators AS (
  SELECT *, CASE WHEN quality_class != prev_quality_class OR is_active != prev_is_active
  THEN 1 ELSE 0 END AS change_ind
  FROM actors_scd_previous
)
, actors_scd_streak AS (
SELECT *,
SUM(change_ind) OVER (PARTITION BY actorid, actor ORDER BY current_year) AS streak_ind
FROM actors_scd_indicators
)
SELECT actorid, actor, quality_class, is_active,
       change_ind,
       streak_ind,
       current_year
--        start_year
FROM actors_scd_streak
where actorid = 'nm0000001'
-- GROUP BY
ORDER BY current_year;

-- 6th version - including films[]

WITH movie_years AS (
SELECT * FROM GENERATE_SERIES(
(SELECT MIN(year) FROM actor_films), (SELECT MAX(year) FROM actor_films)
) AS years
),
first_movie_year AS (
SELECT actorid, actor, MIN(year) first_year, MAX(year) last_year
FROM actor_films GROUP BY actorid, actor
),
movie_and_years AS (
SELECT * FROM first_movie_year fmy
JOIN movie_years my ON fmy.first_year <= my.years
)
-- SELECT * FROM movie_years;
-- SELECT * FROM movie_and_years;
-- SELECT * FROM movie_and_years WHERE actorid = 'nm0000003';
, avg_rating AS (
SELECT actorid, actor, year, AVG(rating) avg_rating
FROM actor_films GROUP BY actorid, actor, year
)
, windowed_actors AS (
SELECT DISTINCT may.actorid, may.actor,
ARRAY_REMOVE( ARRAY_AGG(
CASE WHEN af.year IS NOT NULL
THEN ROW(af.film, af.votes, af.rating, af.filmid)::films
END) OVER (PARTITION BY may.actorid, may.actor ORDER BY may.years)
, NULL) AS films,
ARRAY_REMOVE( ARRAY_AGG(
CASE WHEN ar.year IS NOT NULL
THEN ar.avg_rating END) OVER (PARTITION BY may.actorid, may.actor ORDER BY may.years)
, NULL) AS avg_rating_array,
CASE WHEN af.year IS NOT NULL THEN TRUE ELSE FALSE END AS is_active,
may.years AS current_year,
may.first_year, may.last_year
FROM movie_and_years may
LEFT JOIN actor_films af ON af.actorid = may.actorid AND af.year = may.years
LEFT JOIN avg_rating ar ON ar.actorid = may.actorid AND ar.year = may.years
)
-- SELECT * FROM windowed_actors WHERE actorid = 'nm0000003' ORDER BY current_year;
, actors_scd_base AS (
SELECT actorid, actor, films, CASE
WHEN avg_rating_array[CARDINALITY(avg_rating_array)] > 8 THEN 'star'
WHEN avg_rating_array[CARDINALITY(avg_rating_array)] > 7 THEN 'good'
WHEN avg_rating_array[CARDINALITY(avg_rating_array)] > 6 THEN 'average'
ELSE 'bad'
END::quality_class AS quality_class, is_active, current_year
FROM windowed_actors
)
SELECT * FROM actors_scd_base where actorid = 'nm0000003' ORDER BY current_year;

SELECT * FROM actors_history_scd WHERE actor_id IN ('nm0000001', 'nm0000003') ORDER BY actor_id, current_year;

-- 5. Incremental query for `actors_history_scd`

DROP TYPE IF EXISTS actor_scd_type;
CREATE TYPE actor_scd_type AS (
  films films[],
  quality_class quality_class,
  is_active BOOLEAN,
  start_date INTEGER,
  end_date INTEGER
);



WITH historical_scd AS (
  SELECT actor_id, actor_name, films, quality_class, is_active, start_date, end_date, current_year
  FROM actors_history_scd WHERE current_year = 2020 AND end_date < 2020 ORDER BY actor_id
),
last_year_scd AS (
 SELECT * FROM actors_history_scd WHERE current_year = 2020 AND end_date >= 2020
),
this_year_data AS (
  SELECT * FROM actors WHERE current_year = 2021
),
unchanged_scd AS (
  SELECT tyd.actor_id, tyd.actor_name, tyd.films, tyd.quality_class, tyd.is_active,
  lys.start_date, lys.end_date, tyd.current_year
  FROM this_year_data tyd
  JOIN last_year_scd lys ON tyd.actor_id = lys.actor_id AND tyd.actor_name = lys.actor_name
  WHERE tyd.quality_class = lys.quality_class AND tyd.is_active = lys.is_active
)
-- select * from unchanged_scd WHERE actor_id IN ('nm0000001', 'nm0000003') ORDER BY actor_id, current_year;
, changed_records AS (
SELECT tyd.actor_id, tyd.actor_name,
UNNEST( ARRAY [
ROW(lys.films, lys.quality_class, lys.is_active, lys.start_date, lys.current_year)::actor_scd_type,
ROW(tyd.films, tyd.quality_class, tyd.is_active, tyd.current_year, 9999)::actor_scd_type]
) AS records, tyd.current_year
FROM this_year_data tyd
JOIN last_year_scd lys ON lys.actor_id = tyd.actor_id AND lys.actor_name = tyd.actor_name
WHERE (tyd.quality_class != lys.quality_class OR tyd.is_active != lys.is_active)
)
-- select * from changed_records ORDER BY actor_id LIMIT 100;
, unnested_changed_records AS (
select actor_id, actor_name, (records).films, (records).quality_class, (records).is_active,
(records).start_date, (records).end_date, current_year from changed_records
)
-- select * from unnested_changed_records WHERE actor_id in ('nm0807743', 'nm0702841') ORDER BY start_date, actor_id LIMIT 500;
-- SELECT * FROM actors WHERE actor_id in ('nm0807743', 'nm0702841') ORDER BY actor_id, current_year; -- current_year = 2021
, new_records AS (
SELECT
  tyd.actor_id, tyd.actor_name, tyd.films, tyd.quality_class, tyd.is_active,
  tyd.current_year AS start_year, 9999 AS end_year, tyd.current_year
FROM this_year_data tyd
LEFT JOIN last_year_scd lys ON lys.actor_id = tyd.actor_id AND lys.actor_name = tyd.actor_name
WHERE lys.actor_id IS NULL
)
-- select * from new_records
SELECT * FROM historical_scd
UNION ALL
SELECT * FROM unchanged_scd
UNION ALL
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_records;
