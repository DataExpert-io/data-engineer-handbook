

INSERT INTO actors
    WITH previous AS (
        SELECT * FROM actors
        WHERE year = :start_year
    ), current AS (
        SELECT * FROM actor_films
        WHERE year = :end_year
    ), current_agg AS (
        SELECT
            c.actorid AS actorid,
            c.actor AS actor,
            ARRAY_AGG(ROW(
                c.film,
                c.votes,
                c.rating,
                c.filmid
            )::films) AS films,
            AVG(c.rating) AS avg_rating,
            c.year
        FROM current c
        GROUP BY c.actorid, c.actor, c.year
    )
    SELECT
        COALESCE(c.actorid, p.actorid) AS actorid,
        COALESCE(c.actor, p.actor) AS actor,
        CASE
            WHEN c.films IS NOT NULL
                THEN p.films || c.films
            ELSE p.films
        END AS films,
        CASE
            WHEN c.avg_rating IS NOT NULL
                THEN CASE
                        WHEN c.avg_rating > 8 THEN 'star'
                        WHEN c.avg_rating > 7 AND c.avg_rating <= 8 THEN 'good'
                        WHEN c.avg_rating > 6 AND c.avg_rating <= 7 THEN 'average'
                        ELSE 'bad'
                    END::quality_class
        END AS quality_class,
        CASE
            WHEN coalesce(array_length(c.films, 1), 0) >= 1
                THEN TRUE
            ELSE FALSE
        END AS is_active,
        COALESCE(c.year, p.year + 1) AS year
    FROM current_agg AS c
        FULL OUTER JOIN previous AS p
            USING(actorid)