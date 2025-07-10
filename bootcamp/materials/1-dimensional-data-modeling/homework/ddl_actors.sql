 CREATE TYPE film_stats AS (
    film TEXT,
    votes INTEGER,
    rating FLOAT,
    filmid TEXT
 );

 CREATE TYPE quality_class AS ENUM ('bad', 'average', 'good', 'star');

 CREATE TABLE actors (
    actor_name TEXT,
    actor_id TEXT,
    films film_stats[],
    quality_class quality_class,
    is_active BOOLEAN,
    current_year INTEGER,
    PRIMARY KEY (actor_id, current_year)
 );