CREATE TYPE film_stats AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);

CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

CREATE TABLE actors (
    actor TEXT NOT NULL,
    films film_stats[],
    quality_class quality_class,
    is_active BOOLEAN,
    year INTEGER NOT NULL
);

select  min(year), 
		max(year)
from actor_films af;

select * from actor_films 
where actorid= 'nm0000092'

DO $$
DECLARE
    last_year INT := 2006;
    current_year INT := 2007;
begin
    WITH current_year_agg AS (
        SELECT
            actor,
            ARRAY_AGG(ROW(film, votes, rating, filmid)::film_stats) AS films,
            AVG(rating) AS avg_rating
        FROM actor_films
        WHERE year = current_year
        GROUP BY actor
    ),
    
    last_year_data AS (
        SELECT * 
        FROM actors 
        WHERE year = last_year
    ),
    
    -- Join last year's data with current year aggregated
    aggregated AS (
        SELECT
            COALESCE(l.actor, f.actor) AS actor,
            COALESCE(l.films, ARRAY[]::film_stats[]) || COALESCE(f.films, ARRAY[]::film_stats[]) AS films,
            CASE
                WHEN f.avg_rating IS NOT NULL THEN
                    CASE
                        WHEN f.avg_rating > 8 THEN 'star'
                        WHEN f.avg_rating > 7 THEN 'good'
                        WHEN f.avg_rating > 6 THEN 'average'
                        ELSE 'bad'
                    END::quality_class
                ELSE l.quality_class
            END AS quality_class,
            f.actor IS NOT NULL AS is_active,
            current_year AS year
        FROM last_year_data l
        FULL OUTER JOIN current_year_agg f ON l.actor = f.actor
    )
    INSERT INTO actors (actor, films, quality_class, is_active, year)
    select actor, films, quality_class, is_active, year FROM aggregated;
END $$;


select *
from actor_films
where actor = 'Mika Boorem'

select max(year) from actors

select actor, unnest(films)::film_stats , year , quality_class , is_active
where actor = 'A.J. Buckley'
order by year,films desc 


