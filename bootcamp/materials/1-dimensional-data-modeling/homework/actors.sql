CREATE TYPE film_stats AS (
                         film TEXT,
                         votes INTEGER,
                         rating REAL,
                         filmid TEXT
                       );
 CREATE TYPE quality_class AS
     ENUM ('bad', 'average', 'good', 'star');


CREATE TABLE actors (
                         actorid TEXT,
                         actor TEXT,
                         films film_stats[],
                         quality_class quality_class,
                         is_active BOOLEAN,
                         current_year INTEGER,
                         PRIMARY KEY (actorid, current_year)
);

