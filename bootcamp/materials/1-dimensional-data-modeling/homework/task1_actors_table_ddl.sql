-- Task 1: DDL for actors table
-- This table stores dimensional data for actors with their films and quality metrics

DROP TYPE IF EXISTS film_struct CASCADE;
CREATE TYPE film_struct AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT,
    year INTEGER
);

DROP TYPE IF EXISTS quality_class_enum CASCADE;
CREATE TYPE quality_class_enum AS ENUM ('star', 'good', 'average', 'bad');

DROP TABLE IF EXISTS actors;
CREATE TABLE actors (
    actor text,
    actorid text PRIMARY KEY,
    films film_struct [],
    quality_class quality_class_enum,
    is_active boolean,
    current_year integer
);
