-- 0. Do exploratory data analysis (EDA)
-- -------------------------------------

SELECT * FROM actor_films ORDER BY actorid ASC, filmid ASC LIMIT 10;

-- Determine the date ranges
SELECT MIN("year") FROM actor_films; -- 1970
SELECT MAX("year") FROM actor_films; -- 2021


-- 1. DDL for the actors table
-- ---------------------------

-- Create a struct for a film
CREATE TYPE film AS (
	"year" INTEGER,
	film TEXT,
	votes INTEGER,
	rating REAL,
	filmid TEXT
);

-- Create an enum representing an actor's performance quality
CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

-- Create the actors table
CREATE TABLE actors (
    actorid TEXT,
    actor TEXT,
    films film[],
    quality_class quality_class,
    is_active BOOLEAN,
    current_year INTEGER,
    PRIMARY KEY (actorid, current_year)
);


-- 2. Cumulative table generation query
-- ------------------------------------

DO $$
DECLARE
    yr INTEGER;
BEGIN
    FOR yr IN 1970..2021 LOOP

        WITH

        -- Last year's snapshot
        yesterday AS (
            SELECT actorid, actor, films, current_year
            FROM actors
            WHERE current_year = yr - 1
        ),

        -- This year's new film entries
        today AS (
            SELECT actorid, actor, film, votes, rating, filmid, "year"
            FROM actor_films
            WHERE "year" = yr
        ),

        -- Combine existing + new film entries
        film_rows AS (
            SELECT y.actorid, y.actor, yr AS current_year,
                   unnest(y.films) AS film_row, FALSE AS is_new
            FROM yesterday y

            UNION ALL

            SELECT t.actorid, t.actor, yr AS current_year,
                   ROW(t.year, t.film, t.votes, t.rating, t.filmid)::film AS film_row,
                   TRUE AS is_new
            FROM today t
        ),

        -- Deduplicate based on filmid
        dedup AS (
            SELECT DISTINCT ON (actorid, (film_row).filmid)
                   actorid, actor, current_year, film_row, is_new
            FROM film_rows
            ORDER BY actorid, (film_row).filmid
        ),

        -- Rebuild film list, calc quality and activity
		aggregated AS (
		    SELECT
		        d.actorid,
		        MAX(d.actor) AS actor,
		        d.current_year,
		        ARRAY_AGG(d.film_row ORDER BY (d.film_row).filmid) AS films,
		        AVG(CASE WHEN d.is_new THEN (d.film_row).rating ELSE NULL END) AS avg_rating,
		        BOOL_OR(d.is_new) AS is_active
		    FROM dedup d
		    GROUP BY d.actorid, d.current_year
		),
		
		-- Pull in yesterday's quality_class for fallback
		with_quality AS (
		    SELECT
		        a.*,
		        y.quality_class AS prev_quality_class
		    FROM aggregated a
		    LEFT JOIN actors y
		        ON a.actorid = y.actorid AND a.current_year = y.current_year + 1
		)
		
		-- Final insert
		INSERT INTO actors (actorid, actor, films, current_year, quality_class, is_active)
		SELECT
		    actorid,
		    actor,
		    films,
		    current_year,
			CASE
			    WHEN avg_rating IS NOT NULL THEN
			        CASE
			            WHEN avg_rating > 8 THEN 'star'::quality_class
			            WHEN avg_rating > 7 THEN 'good'::quality_class
			            WHEN avg_rating > 6 THEN 'average'::quality_class
			            ELSE 'bad'::quality_class
			        END
			    ELSE prev_quality_class
			END AS quality_class,
		    is_active
		FROM with_quality
		ON CONFLICT (actorid, current_year)
		DO UPDATE
		    SET films = EXCLUDED.films,
		        quality_class = EXCLUDED.quality_class,
		        is_active = EXCLUDED.is_active;

    END LOOP;
END $$;


-- Pipeline version doing 1 year at a time
-- ---------------------------------------

-- INSERT INTO actors
-- WITH

-- -- Last year's snapshot
-- yesterday AS (
-- 	SELECT actorid, actor, films, quality_class, is_active, current_year
-- 	FROM actors
-- 	WHERE current_year = 1969
-- ),

-- -- This year's new film entries
-- today AS (
-- 	SELECT actorid, actor, film, votes, rating, filmid, "year"
-- 	FROM actor_films
-- 	WHERE "year" = 1970
-- )

-- SELECT 
-- 	COALESCE(t.actorid, y.actorid) AS actorid,
-- 	COALESCE(t.actor, y.actor) AS actor,
-- 	CASE
-- 		WHEN y.films IS NULL THEN
-- 			ARRAY[ROW(
-- 				t."year",
-- 				t.film,
-- 				t.votes,
-- 				t.rating,
-- 				t.filmid
-- 			)::film]
-- 		WHEN t."year" IS NOT NULL THEN
-- 			y.films || ARRAY[ROW(
-- 				t."year",
-- 				t.film,
-- 				t.votes,
-- 				t.rating,
-- 				t.filmid
-- 			)::film]
-- 		ELSE
-- 			y.films
-- 	END AS films,
-- 	CASE
-- 		WHEN t."year" IS NOT NULL THEN
-- 			CASE
-- 				WHEN t.rating > 8 THEN 'star'
-- 				WHEN t.rating > 7 THEN 'good'
-- 				WHEN t.rating > 6 THEN 'average'
-- 				ELSE 'bad'
-- 			END::quality_class
-- 		ELSE
-- 			y.quality_class
-- 	END AS quality_class,
-- 	CASE
-- 		WHEN t."year" IS NOT NULL THEN
-- 			true
-- 		ELSE
-- 			false
-- 	END AS is_active,
-- 	COALESCE(t."year", y.current_year + 1) as current_year
		
-- 	FROM today t FULL OUTER JOIN yesterday y
-- 		ON t.actorid = y.actorid;

-- SELECT * FROM actors WHERE current_year=1981


-- Recover the original actor_films and add quality_class and is_active
WITH unnested AS (
    SELECT actor, actorid, current_year AS "year",
           UNNEST(films)::film AS films, quality_class, is_active
    FROM actors
    -- WHERE current_year = 1971 AND actor = 'Les novices'
)
SELECT actor, actorid, (films::film).film, "year",
       (films::film).votes, (films::film).rating,
       quality_class, is_active
FROM unnested;



-- 3. DDL for `actors_history_scd` table
-- -------------------------------------

CREATE TABLE actors_history_scd (
    actor TEXT,
    quality_class quality_class,
    is_active BOOLEAN,
    start_date INTEGER,
    end_date INTEGER
);


-- 4. Backfill query for `actors_history_scd`
-- ------------------------------------------

WITH streak_started AS (
    SELECT actor, quality_class, is_active, current_year,
           LAG(quality_class) OVER (PARTITION BY actor ORDER BY current_year) <> quality_class
           OR LAG(quality_class) OVER (PARTITION BY actor ORDER BY current_year) IS NULL AS did_change
    FROM actors
),
streak_identified AS (
    SELECT actor, quality_class, is_active, current_year,
           SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
           OVER (PARTITION BY actor ORDER BY current_year) AS streak_identifier
    FROM streak_started
),
aggregated AS (
    SELECT actor, quality_class, is_active, streak_identifier,
           MIN(current_year) AS start_date,
           MAX(current_year) AS end_date
    FROM streak_identified
    GROUP BY 1, 2, 3, 4
)

INSERT INTO actors_history_scd
SELECT actor, quality_class, is_active, start_date, end_date
FROM aggregated;



-- 5. Incremental query for `actors_history_scd`
-- ---------------------------------------------

CREATE TYPE scd_type AS (
    quality_class quality_class,
    is_active BOOLEAN,
    start_date INTEGER,
    end_date INTEGER
);

DO $$
DECLARE
    yr INTEGER;
BEGIN
    FOR yr IN 1970..2021 LOOP

        -- 1. Extend existing unchanged actors' rows
        WITH this_year AS (
            SELECT *
            FROM actors
            WHERE current_year = yr
        ),
        last_year_scd AS (
            SELECT DISTINCT ON (actor)
                   actor, quality_class, is_active, start_date, end_date
            FROM actors_history_scd
            WHERE end_date = yr - 1
            ORDER BY actor, end_date DESC
        ),
        unchanged AS (
            SELECT t.actor, t.quality_class, t.is_active,
                   l.start_date AS start_date,
                   yr AS end_date
            FROM this_year t
            JOIN last_year_scd l ON t.actor = l.actor
            WHERE t.quality_class = l.quality_class
              AND t.is_active = l.is_active
        )
        INSERT INTO actors_history_scd (actor, quality_class, is_active, start_date, end_date)
        SELECT * FROM unchanged;


        -- 2. Close previous rows and open new ones for changed actors
        WITH this_year AS (
            SELECT *
            FROM actors
            WHERE current_year = yr
        ),
        last_year_scd AS (
            SELECT DISTINCT ON (actor)
                   actor, quality_class, is_active, start_date, end_date
            FROM actors_history_scd
            WHERE end_date = yr - 1
            ORDER BY actor, end_date DESC
        ),
        changed AS (
            SELECT t.actor,
                   l.quality_class AS prev_quality_class,
                   l.is_active AS prev_is_active,
                   l.start_date AS prev_start,
                   yr - 1 AS prev_end,
                   t.quality_class AS new_quality_class,
                   t.is_active AS new_is_active,
                   yr AS new_start,
                   yr AS new_end
            FROM this_year t
            JOIN last_year_scd l ON t.actor = l.actor
            WHERE t.quality_class <> l.quality_class
               OR t.is_active <> l.is_active
        )
        INSERT INTO actors_history_scd (actor, quality_class, is_active, start_date, end_date)
        SELECT actor, prev_quality_class, prev_is_active, prev_start, prev_end FROM changed
        UNION ALL
        SELECT actor, new_quality_class, new_is_active, new_start, new_end FROM changed;


        -- 3. Insert brand new actors
        WITH this_year AS (
            SELECT *
            FROM actors
            WHERE current_year = yr
        ),
        last_year_scd AS (
            SELECT DISTINCT ON (actor)
                   actor
            FROM actors_history_scd
            WHERE end_date = yr - 1
        ),
        new_entries AS (
            SELECT t.actor, t.quality_class, t.is_active,
                   yr AS start_date,
                   yr AS end_date
            FROM this_year t
            LEFT JOIN last_year_scd l ON t.actor = l.actor
            WHERE l.actor IS NULL
        )
        INSERT INTO actors_history_scd (actor, quality_class, is_active, start_date, end_date)
        SELECT * FROM new_entries;

    END LOOP;
END $$;
