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

INSERT INTO actors
WITH last_year AS (
  SELECT actor_id, actor_name, films, quality_class, current_year
  FROM actors WHERE current_year = 1971
),
     this_year AS (
       SELECT actorid, actor,
              ARRAY_AGG(row(film, votes, rating, filmid)::films) as films,
              AVG(rating) AS avg_rating, TRUE AS is_active, year
       FROM actor_films WHERE year = 1972
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

-- 3. DDL for `actors_history_scd` table

DROP TABLE IF EXISTS actors_history_scd;
CREATE TABLE actors_history_scd (
  actor_id TEXT, actor_name TEXT, films films[], quality_class quality_class,
  is_active BOOLEAN, start_date INTEGER, end_date INTEGER, current_year INTEGER,
  PRIMARY KEY (actor_id, start_date)
);

-- 4. Backfill query for `actors_history_scd`

INSERT INTO actors_history_scd
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
SELECT * FROM first_movie_year fmy
JOIN movie_years my ON fmy.first_year <= my.years
)
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
CASE WHEN ar.year IS NOT NULL THEN TRUE ELSE FALSE END AS is_active,
may.years AS current_year
FROM movie_and_years may
LEFT JOIN actor_films af ON af.actorid = may.actorid AND af.year = may.years
LEFT JOIN avg_rating ar ON ar.actorid = may.actorid AND ar.year = may.years
)
, actors_scd_base AS (
SELECT actorid, actor, films, CASE
WHEN avg_rating_array[CARDINALITY(avg_rating_array)] > 8 THEN 'star'
WHEN avg_rating_array[CARDINALITY(avg_rating_array)] > 7 THEN 'good'
WHEN avg_rating_array[CARDINALITY(avg_rating_array)] > 6 THEN 'average'
ELSE 'bad'
END::quality_class AS quality_class, is_active, current_year
FROM windowed_actors
)
, actors_scd_previous AS (
SELECT *,
LAG(quality_class, 1) OVER (PARTITION BY actorid, actor ORDER BY current_year) AS prev_quality_class,
LAG(is_active, 1) OVER (PARTITION BY actorid, actor ORDER BY current_year) AS prev_is_active
FROM actors_scd_base
)
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
, actors_scd_grouped AS (
  SELECT actorid, actor, films, quality_class, is_active,
  MIN(current_year) AS start_year, MAX(current_year) AS end_year,
  (SELECT MAX(year) - 1 FROM actor_films) AS current_year
  FROM actors_scd_streak
  GROUP BY actorid, actor, films, quality_class, is_active, streak_ind
)
SELECT actorid, actor, films, quality_class, is_active, start_year,
       CASE WHEN end_year = current_year THEN 9999 ELSE end_year END AS end_year,
       current_year
FROM actors_scd_grouped ORDER BY start_year
;




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
  FROM actors_history_scd WHERE current_year = 2020 AND end_date < 2020
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
),
changed_records AS (
SELECT tyd.actor_id, tyd.actor_name,
UNNEST( ARRAY [
ROW(lys.films, lys.quality_class, lys.is_active, lys.start_date, lys.current_year)::actor_scd_type,
ROW(tyd.films, tyd.quality_class, tyd.is_active, tyd.current_year, 9999)::actor_scd_type]
) AS records, tyd.current_year
FROM this_year_data tyd
JOIN last_year_scd lys ON lys.actor_id = tyd.actor_id AND lys.actor_name = tyd.actor_name
WHERE (tyd.quality_class != lys.quality_class OR tyd.is_active != lys.is_active)
),
unnested_changed_records AS (
select actor_id, actor_name, (records).films, (records).quality_class, (records).is_active,
(records).start_date, (records).end_date, current_year from changed_records
),
new_records AS (
SELECT
tyd.actor_id, tyd.actor_name, tyd.films, tyd.quality_class, tyd.is_active,
tyd.current_year AS start_year, 9999 AS end_year, tyd.current_year
FROM this_year_data tyd
LEFT JOIN last_year_scd lys ON lys.actor_id = tyd.actor_id AND lys.actor_name = tyd.actor_name
WHERE lys.actor_id IS NULL
)
SELECT * FROM historical_scd
UNION ALL
SELECT * FROM unchanged_scd
UNION ALL
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_records;

