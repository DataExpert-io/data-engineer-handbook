CREATE TYPE films AS (
    film text,
    filmid text,
    votes Integer,
    rating real
);

CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');


CREATE TABLE actors (
    actor text,
    actorid text,
    current_year integer,
    films films [],
    quality_class quality_class [],
    is_active boolean
);
