-- SELECT MIN(year), MAX(year) FROM actor_films; -- (1970, 2021)

WITH last_year AS (
    SELECT * FROM actors 
    WHERE current_year = 1980
),
this_year AS (
    SELECT 
        actor,
        actorid,
        year,
        ARRAY_AGG(ROW(film,
                    votes,
                    rating,
                    filmid)::film_stats) AS films,
        MAX(CASE
                WHEN rating > 8 THEN 'star'
                WHEN rating BETWEEN 7.1 AND 8 THEN 'good'
                WHEN rating BETWEEN 6.1 AND 7 THEN 'average'
                ELSE 'bad' 
            END)::quality_class AS quality_class
    FROM actor_films
    WHERE year = 1981
    GROUP BY actor, actorid, year
)
INSERT INTO actors
SELECT
    COALESCE(ly.actor_name, ty.actor) AS actor_name,
    COALESCE(ly.actor_id, ty.actorid) AS actor_id,
    COALESCE(ly.films, ARRAY[]::film_stats[])
        || COALESCE(ty.films, ARRAY[]::film_stats[]) AS films,
    COALESCE(ly.quality_class, ty.quality_class) AS quality_class,
    ty.year IS NOT NULL AS is_active,
    1981 AS current_year
FROM last_year ly
FULL OUTER JOIN this_year ty
ON ly.actor_id = ty.actorid;