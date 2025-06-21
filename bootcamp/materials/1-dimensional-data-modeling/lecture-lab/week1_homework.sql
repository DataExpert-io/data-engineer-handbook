---DDL for actors table

CREATE TABLE actors (
    actorid INTEGER PRIMARY KEY,
    films JSONB, -- array of structs as JSON
    quality_class TEXT,
    is_active BOOLEAN
);


---Cumulative Table Generation Query

INSERT INTO actors (actorid, films, quality_class, is_active)
SELECT
    af.actorid,
    JSONB_AGG(
        JSONB_BUILD_OBJECT(
            'film', af.film,
            'votes', af.votes,
            'rating', af.rating,
            'filmid', af.filmid
        )
    ) AS films,
    CASE
        WHEN AVG(af.rating) > 8 THEN 'star'
        WHEN AVG(af.rating) > 7 THEN 'good'
        WHEN AVG(af.rating) > 6 THEN 'average'
        ELSE 'bad'
    END AS quality_class,
    MAX(af.year) = EXTRACT(YEAR FROM CURRENT_DATE) AS is_active
FROM actor_films af
GROUP BY af.actorid;

---DDL for actors_history_scd table (Type 2 SCD)

CREATE TABLE actors_history_scd (
    actorid INTEGER,
    quality_class TEXT,
    is_active BOOLEAN,
    start_date DATE,
    end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (actorid, start_date)
);

---Backfill Query for actors_history_scd

INSERT INTO actors_history_scd (actorid, quality_class, is_active, start_date, end_date, is_current)
SELECT
    af.actorid,
    CASE
        WHEN AVG(af.rating) > 8 THEN 'star'
        WHEN AVG(af.rating) > 7 THEN 'good'
        WHEN AVG(af.rating) > 6 THEN 'average'
        ELSE 'bad'
    END AS quality_class,
    MAX(af.year) = EXTRACT(YEAR FROM CURRENT_DATE) AS is_active,
    DATE_TRUNC('year', TO_DATE(af.year::TEXT, 'YYYY')) AS start_date,
    NULL AS end_date, 
    TRUE AS is_current
FROM actor_films af
GROUP BY af.actorid, af.year;

---Incremental Query for actors_history_scd

WITH new_data AS (
    SELECT actorid, quality_class, is_active
    FROM actors
),
changed AS (
    SELECT n.*
    FROM new_data n
    LEFT JOIN actors_history_scd h
        ON n.actorid = h.actorid AND h.is_current = TRUE
    WHERE n.quality_class IS DISTINCT FROM h.quality_class
       OR n.is_active IS DISTINCT FROM h.is_active
)
-- Close old record
UPDATE actors_history_scd
SET end_date = CURRENT_DATE - INTERVAL '1 day',
    is_current = FALSE
WHERE actorid IN (SELECT actorid FROM changed)
  AND is_current = TRUE;

-- Insert new record
INSERT INTO actors_history_scd (actorid, quality_class, is_active, start_date, end_date, is_current)
SELECT
    actorid,
    quality_class,
    is_active,
    CURRENT_DATE,
    NULL,
    TRUE
FROM changed;
