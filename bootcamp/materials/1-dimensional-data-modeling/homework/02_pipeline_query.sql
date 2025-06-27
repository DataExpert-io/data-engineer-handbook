WITH last_state AS (
    SELECT *
    FROM actors
),
this_year_films AS (
    SELECT *
    FROM actor_films
    WHERE year = 1998
),
aggregated_this_year AS (
    SELECT
        actorid::TEXT AS actorid,
        actor,
        ARRAY_AGG(ROW(film, votes, rating, filmid)::film_info ORDER BY film) AS film_array,
        AVG(rating) AS avg_rating
    FROM this_year_films
    GROUP BY actorid, actor
),
incoming_data AS (
    SELECT
        COALESCE(ls.actorid, aty.actorid) AS actorid,
        COALESCE(ls.actor, aty.actor) AS actor,
        COALESCE(ls.films, ARRAY[]::film_info[]) ||
            COALESCE(aty.film_array, ARRAY[]::film_info[]) AS films,
        CASE
            WHEN aty.avg_rating > 8 THEN 'star'
            WHEN aty.avg_rating > 7 THEN 'good'
            WHEN aty.avg_rating > 6 THEN 'average'
            WHEN aty.avg_rating IS NOT NULL THEN 'bad'
            ELSE ls.quality_class
        END::quality_class_enum AS quality_class,
        aty.actorid IS NOT NULL AS is_active
    FROM last_state ls
    FULL OUTER JOIN aggregated_this_year aty
        ON ls.actorid = aty.actorid
)

INSERT INTO actors (actorid, actor, films, quality_class, is_active)
SELECT
    actorid,
    actor,
    ARRAY(
        SELECT DISTINCT f
        FROM unnest(films) AS f
    ) AS films,
    quality_class,
    is_active
FROM incoming_data
ON CONFLICT (actorid) DO UPDATE SET
    actor = EXCLUDED.actor,
    films = ARRAY(
        SELECT DISTINCT f
        FROM unnest(EXCLUDED.films) AS f
    ),
    quality_class = EXCLUDED.quality_class,
    is_active = EXCLUDED.is_active;