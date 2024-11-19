MERGE INTO actors AS a
USING (
    WITH last_year AS (
        SELECT
            actor,
            actorid,
            current_year,
            films,
            quality_class,
            is_active
        FROM actors
        WHERE current_year = 1973
    ),

    current_year_table AS (
        SELECT
            actor,
            actorid,
            year,
            film,
            votes,
            rating,
            filmid
        FROM actor_films
        WHERE year = 1974
    )

    SELECT
        COALESCE(l.actor, c.actor) AS actor,
        COALESCE(l.actorid, c.actorid) AS actorid,
        COALESCE(l.films, ARRAY[]::films [])
        || CASE
            WHEN c.year IS NOT NULL
                THEN
                    ARRAY_AGG(ROW(c.film, c.votes, c.rating, c.filmid)::films)
            ELSE ARRAY[]::films []
        END AS films,
        CASE
            WHEN c.year IS NOT NULL
                THEN
                    ARRAY[CASE
                        WHEN AVG(c.rating) > 8 THEN 'star'
                        WHEN AVG(c.rating) > 7 THEN 'good'
                        WHEN AVG(c.rating) > 6 THEN 'average'
                        ELSE 'bad'
                    END::quality_class]
            ELSE l.quality_class
        END AS quality_class,
        c.year IS NOT NULL AS is_active,
        1974 AS current_year
    FROM current_year_table AS c
    FULL OUTER JOIN last_year AS l ON c.actorid = l.actorid
    GROUP BY
        l.actor, l.actorid, c.actorid, c.actor, l.films, c.year, l.quality_class
) AS source
    ON (source.actorid = a.actorid AND source.current_year = a.current_year)
WHEN MATCHED THEN
    UPDATE SET
        actor = source.actor,
        films = source.films,
        quality_class = source.quality_class,
        is_active = source.is_active
WHEN NOT MATCHED THEN
    INSERT (actor, actorid, films, quality_class, is_active, current_year)
    VALUES (
        source.actor,
        source.actorid,
        source.films,
        source.quality_class,
        source.is_active,
        source.current_year
    );
