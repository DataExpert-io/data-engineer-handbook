-- Task 2: Cumulative table generation query
-- Write a query that populates the actors table one year at a time
-- This implements an incremental cumulative table pattern combining yesterday's data with today's new data

DO
$$
    DECLARE
        y_year INT;
        t_year INT;
    BEGIN
        FOR y_year IN 1969..2020
            LOOP
                t_year := y_year + 1;
                INSERT INTO actors
                WITH yesterday AS (SELECT *
                                   FROM actors
                                   WHERE current_year = y_year),

                     today_aggregated AS (SELECT actorid,
                                                 MAX(year)                                                  AS year,
                                                 MAX(actor)                                                 AS actor,
                                                 ARRAY_AGG(ROW (film, votes, rating, filmid)::film_details) AS films,
                                                 AVG(rating)                                                AS average_rating
                                          FROM actor_films
                                          WHERE year = t_year
                                          GROUP BY actorid)

                SELECT COALESCE(t.actorid, y.actorid)       AS actorid,
                       COALESCE(t.actor, y.actor)           AS actor,
                       CASE
                           WHEN y.films IS NULL THEN t.films
                           WHEN t.films IS NOT NULL THEN y.films || t.films
                           ELSE y.films
                           END                              AS films,
                       CASE
                           WHEN t.films IS NOT NULL THEN
                               (CASE
                                    WHEN t.average_rating > 8 THEN 'star'
                                    WHEN t.average_rating > 7 THEN 'good'
                                    WHEN t.average_rating > 6 THEN 'average'
                                    ELSE 'bad'
                                   END)::actor_quality_class
                           ELSE y.quality_class
                           END                              AS quality_class,

                       t.films IS NOT NULL                  AS is_active,
                       COALESCE(t.year, y.current_year + 1) AS current_year
                FROM today_aggregated t
                         FULL OUTER JOIN yesterday y ON t.actorid = y.actorid;

            END LOOP;
    END
$$;
