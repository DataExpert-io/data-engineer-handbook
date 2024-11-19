CREATE TABLE actor_films (
    actor TEXT,
    actorid TEXT,
    film TEXT,
    year INTEGER,
    votes INTEGER,
    rating REAL,
    filmid TEXT,
    PRIMARY KEY (actorid, filmid)
)
