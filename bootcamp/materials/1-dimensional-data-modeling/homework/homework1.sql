/*---------------------
Task 1 DDL for actors table
 ---------------------*/

--create films field

CREATE TYPE films_type AS (
                    film TEXT,
					votes INTEGER,
					rating FLOAT4,
					filmid TEXT
                    );
                   
--create quality_class
                   
CREATE TYPE quality_class AS
     ENUM ('bad', 'average', 'good', 'star');
    
    
-- create actors table 
 
CREATE TABLE actors(
actorid TEXT,
actor TEXT,
current_year INT,
films films_type[],
quality_class quality_class,
is_active BOOLEAN,
PRIMARY KEY (actorid,current_year )
);


/*---------------------
 Task 2 Cumulative table generation query
 ---------------------*/

WITH last_year AS (
    SELECT
        actorid,
        actor,
        current_year,
        films,
        quality_class,
        is_active
    FROM
        actors
    WHERE
        current_year = 1971
),
current_year AS (
    SELECT
        actorid,
         actor,  
        year,
        
        -- aggregate the films per actor per year
        
        CASE WHEN year IS NULL THEN ARRAY[]::films_type[]
         ELSE ARRAY_AGG(ROW(film, votes, rating, filmid)::films_type)
    END AS films,
    
    -- getting average rating from all movies per year
        AVG(rating) AS avg_rating
    FROM
        actor_films
    WHERE
        year = 1972
    GROUP BY
        actorid,
        actor,
        year
       
)
INSERT INTO actors
SELECT
    COALESCE(ly.actorid, cy.actorid) AS actorid,
    COALESCE(ly.actor, cy.actor) AS actor,
    COALESCE(cy.year,ly.current_year + 1) as current_year,
    COALESCE(ly.films, ARRAY[]::films_type[]) || COALESCE(cy.films, ARRAY[]::films_type[]) AS films,
    CASE
        WHEN cy.films IS NOT NULL THEN
            CASE
                WHEN cy.avg_rating > 8 THEN 'star'
                WHEN cy.avg_rating > 7 THEN 'good'
                WHEN cy.avg_rating > 6 THEN 'average'
                ELSE 'bad'
            END::quality_class
        ELSE
            ly.quality_class
    END AS quality_class,
    (cy.films IS NOT NULL) AS is_active
FROM
    last_year ly
FULL OUTER JOIN
    current_year cy ON ly.actorid = cy.actorid;

  /* test queryt to check records*/
   SELECT
    a.actorid,
    a.actor,
    a.current_year,
    f.film,
    f.votes,
    f.rating,
    f.filmid
FROM
    actors a,
    UNNEST(a.films) AS f(film, votes, rating, filmid)
WHERE
    a.actorid = 'nm0000020'and
   a.current_year=1970
 
/*---------------------
Task 3 DDL for actors_history_scd table
 ---------------------*/

CREATE TABLE actors_history_scd
(

	actorid TEXT, 
	actor TEXT,
	quality_class quality_class,
	is_active boolean,
	start_date integer,
	end_date integer,
	current_year INTEGER,
	PRIMARY KEY (actorid, start_date)
);

/*---------------------
Task 4 DDL Backfill query for actors_history_scd
 ---------------------*/

INSERT INTO actors_history_scd (actorid, actor, quality_class, is_active, start_date, end_date, current_year)
WITH streak_started AS (
    SELECT
        actorid,
        actor,
        current_year,
        quality_class,
        is_active,
        CASE
            WHEN LAG(quality_class) OVER (PARTITION BY actorid ORDER BY current_year) IS DISTINCT FROM quality_class
                 OR LAG(is_active) OVER (PARTITION BY actorid ORDER BY current_year) IS DISTINCT FROM is_active
                 OR LAG(actorid) OVER (PARTITION BY actorid ORDER BY current_year) IS NULL
            THEN 1
            ELSE 0
        END AS did_change
    FROM actors
),
streak_identified AS (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        current_year,
        SUM(did_change) OVER (PARTITION BY actorid ORDER BY current_year) AS streak_id
    FROM streak_started
),

aggregated AS (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        MIN(current_year) AS start_date,
        MAX(current_year) AS end_date,
        MAX(current_year) AS current_year
		 
    FROM streak_identified
    GROUP BY
        actorid,
        actor,
        quality_class,
        is_active
)
SELECT
    actorid,
    actor,
    quality_class,
    is_active,
    start_date,
    end_date,
    current_year
FROM aggregated
ORDER BY actor, start_date;

/*---------------------
Task 5 Incremental query for actors_history_scd
 ---------------------*/

-- Create type
CREATE TYPE scd_type AS (
    quality_class quality_class,
    is_active BOOLEAN,
    start_date INTEGER,
    end_date INTEGER
);


--- Incremental query for actors_history_scd
WITH last_year_scd AS (
    SELECT * FROM actors_history_scd
    WHERE current_year = 1971
    AND end_date = 1971
),
historical_scd AS (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        start_date,
        end_date
    FROM actors_history_scd
    WHERE current_year = 1971
    AND end_date < 1971
),
this_year_data AS (
    SELECT * FROM actors
    WHERE current_year = 1972
),
unchanged_records AS (
    SELECT
        ty.actorid,
        ty.actor,
        ty.quality_class,
        ty.is_active,
        ly.start_date,
        ty.current_year AS end_date
    FROM this_year_data ty
    JOIN last_year_scd ly
        ON ly.actorid = ty.actorid
    WHERE ty.quality_class = ly.quality_class
        AND ty.is_active = ly.is_active
),
changed_records AS (
    SELECT
        ty.actorid,
        ty.actor,
        UNNEST(ARRAY[
            ROW(
                ly.quality_class,
                ly.is_active,
                ly.start_date,
                ly.end_date
            )::scd_type,
            ROW(
                ty.quality_class,
                ty.is_active,
                ty.current_year,
                ty.current_year
            )::scd_type
        ]) AS records
    FROM this_year_data ty
    LEFT JOIN last_year_scd ly
        ON ly.actorid = ty.actorid
    WHERE ty.quality_class <> ly.quality_class
        OR ty.is_active <> ly.is_active
),
unnested_changed_records AS (
    SELECT
        actorid,
        actor,
        (records::scd_type).quality_class,
        (records::scd_type).is_active,
        (records::scd_type).start_date,
        (records::scd_type).end_date
    FROM changed_records
),
new_records AS (
    SELECT
        ty.actorid,
        ty.actor,
        ty.quality_class,
        ty.is_active,
        ty.current_year AS start_date,
        ty.current_year AS end_date
    FROM this_year_data ty
    LEFT JOIN last_year_scd ly
        ON ty.actorid = ly.actorid
    WHERE ly.actorid IS NULL
)
SELECT *, 1972 AS current_year FROM (
    SELECT * FROM historical_scd
    UNION ALL
    SELECT * FROM unchanged_records
    UNION ALL
    SELECT * FROM unnested_changed_records
    UNION ALL
    SELECT * FROM new_records
) AS combined_records;


