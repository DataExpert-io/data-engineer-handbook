-- Task 1: DDL for actors table
-- Create a DDL for an actors table with the following fields:
--   - films: An array of struct with film details (film, votes, rating, filmid)
--   - quality_class: Actor's performance quality based on average rating of most recent year
--   - is_active: Boolean indicating if actor is currently active (making films this year)

CREATE TYPE film_details AS
(
    film   TEXT,
    votes  INTEGER,
    rating REAL,
    filmid TEXT
);

CREATE TYPE actor_quality_class AS ENUM ('star', 'good', 'average', 'bad');

CREATE TABLE actors
(
    actorid       TEXT,
    actor         TEXT,
    films         film_details[],
    quality_class actor_quality_class,
    is_active     BOOLEAN,
    current_year  INTEGER,
    PRIMARY KEY (actorid, current_year)
);
