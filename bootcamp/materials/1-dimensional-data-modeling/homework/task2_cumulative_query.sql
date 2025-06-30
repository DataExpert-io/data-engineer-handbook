-- Task 2: Example usage of the cumulative table generation query
-- This shows how to populate the actors table year by year with UPSERT logic

INSERT INTO actors (
    actor, actorid, films, quality_class, is_active, current_year
)
WITH target_year_data AS (
    SELECT 1971 AS processing_year
),

-- Get films for actors in the target year
current_year_films AS (
    SELECT
        actor,
        actorid,
        year,
        ARRAY_AGG(
            ROW(film, votes, rating, filmid, year)::film_struct
        ) AS films_this_year,
        AVG(rating) AS avg_rating_this_year
    FROM actor_films AS af
    CROSS JOIN target_year_data AS tyd
    WHERE af.year = tyd.processing_year
    GROUP BY actor, actorid, year
),

-- Get existing actor data (if any) plus new films
actor_updates AS (
    SELECT
        COALESCE(a.actor, cyf.actor) AS actor,
        COALESCE(a.actorid, cyf.actorid) AS actorid,
        tyd.processing_year AS current_year,
        -- Combine existing films with new films from current year
        CASE
            WHEN a.films IS NULL THEN cyf.films_this_year
            ELSE a.films || cyf.films_this_year
        END AS films,
        -- Quality class is based on most recent year's average rating
        CASE
            WHEN cyf.avg_rating_this_year > 8 THEN 'star'::quality_class_enum
            WHEN cyf.avg_rating_this_year > 7 THEN 'good'::quality_class_enum
            WHEN cyf.avg_rating_this_year > 6 THEN 'average'::quality_class_enum
            ELSE 'bad'::quality_class_enum
        END AS quality_class,
        -- Active if they have films in the current year
        cyf.actorid IS NOT NULL AS is_active
    FROM current_year_films AS cyf
    CROSS JOIN target_year_data AS tyd
    LEFT JOIN actors AS a ON cyf.actorid = a.actorid
)

SELECT
    actor,
    actorid,
    films,
    quality_class,
    is_active,
    current_year
FROM actor_updates
ON CONFLICT (actorid)
DO UPDATE SET
    films = excluded.films,
    quality_class = excluded.quality_class,
    is_active = excluded.is_active,
    current_year = excluded.current_year;
