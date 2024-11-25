CREATE TYPE films AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);

CREATE TYPE quality_class AS ENUM('bad', 'average', 'good', 'star');

CREATE TABLE actors (
    actorid TEXT,
    actor TEXT,
    films films[],
    quality_class quality_class,
    is_active bool,
    year INT,
    PRIMARY KEY (actorid, year)
);