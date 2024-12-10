WITH current_year_film AS (
    SELECT
    actorid,
    ARRAY_AGG(ROW(
            film::TEXT,
            votes::INTEGER,
            rating::FLOAT,
            filmid::TEXT
        )::film_type) AS film_r, -- Cast explicitly
    year
    FROM actor_films
    WHERE year = 1996
    GROUP BY actorid, year
),
merged_actors as (
    SELECT
        -- cf.*
        COALESCE(lf.actorid, cf.actorid) AS actorid,
        COALESCE(lf.films, ARRAY[]::film_type[]) ||
        CASE
            WHEN cf.actorid IS NOT NULL THEN cf.film_r
            ELSE ARRAY[]::film_type[]
        END as films,
        (cf.year IS NOT NULL)::BOOLEAN as is_active
    FROM actors lf
    FULL OUTER JOIN current_year_film cf
    ON lf.actorid = cf.actorid
    ORDER BY actorid
),
-- Step 3: Compute average rating for each actor
actors_with_avg AS (
    SELECT
        actorid,
        films,
        is_active,
        (SELECT AVG(film.rating)
         FROM UNNEST(films) AS film) AS avg_rating -- Corrected AVG computation
    FROM merged_actors
),
-- Step 4: Assign quality_class based on avg_rating
final_actors AS (
    SELECT
        actorid,
        films,
        is_active,
        CASE
            WHEN avg_rating > 8 THEN 'star'::quality_type
            WHEN avg_rating > 7 THEN 'good'::quality_type
            WHEN avg_rating > 6 THEN 'average'::quality_type
            ELSE 'bad'::quality_type
        END AS quality_class
    FROM actors_with_avg
)
-- Insert new data into the actors table
INSERT INTO actors (actorid, films, is_active, quality_class)
SELECT
    actorid,
    films,
    is_active,
    quality_class
FROM final_actors
ON CONFLICT (actorid) DO UPDATE
SET
    films = EXCLUDED.films,
    is_active = EXCLUDED.is_active,
    quality_class = EXCLUDED.quality_class;