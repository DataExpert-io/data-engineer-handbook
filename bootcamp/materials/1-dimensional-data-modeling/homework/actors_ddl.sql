
-- DROP TYPE IF EXISTS film_type

CREATE TYPE film_type AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
)

CREATE TYPE quality_type
    AS ENUM('bad', 'average', 'good', 'star');


-- DROP TABLE  IF EXISTS
-- actors;

CREATE TABLE IF NOT EXISTS actors  (
    actorid TEXT,
    films film_type[],
    is_active BOOLEAN,
    quality_class quality_type,
    PRIMARY KEY (actorid));