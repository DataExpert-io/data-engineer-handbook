CREATE TYPE film_info AS (
    film    TEXT,
    votes   INT,
    rating  NUMERIC,
    filmid  TEXT
);

CREATE TYPE quality_class_enum AS 
    ENUM ('star', 'good', 'average', 'bad');


CREATE TABLE actors (
    actorid       TEXT PRIMARY KEY,
    actor         TEXT NOT NULL,
    films         film_info[],           -- array of structured film records
    quality_class quality_class_enum,    -- ENUM type
    is_active     BOOLEAN NOT NULL
);
