DROP TABLE IF EXISTS actors_history_scd;


CREATE TABLE IF NOT EXISTS actors_history_scd (
    scd_id SERIAL PRIMARY KEY,
    actorid TEXT NOT NULL,
    films film_type[],
    quality_class quality_type,
    is_active BOOLEAN,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    current_flag BOOLEAN DEFAULT TRUE,
    UNIQUE (actorid, start_date)
);
