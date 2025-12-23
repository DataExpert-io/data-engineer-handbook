-- Task 3: DDL for actors_history_scd table
-- Create a DDL for an actors_history_scd table with the following features:
--   - Implements type 2 dimension modeling (includes start_date and end_date fields)
--   - Tracks quality_class and is_active status for each actor in the actors table

CREATE TYPE actors_scd_type AS
(
    quality_class actor_quality_class,
    is_active     BOOLEAN,
    start_date    INTEGER,
    end_date      INTEGER
);

CREATE TABLE actors_history_scd
(
    actorid       TEXT,
    quality_class actor_quality_class,
    is_active     BOOLEAN,
    start_date    INTEGER,
    end_date      INTEGER,
    current_year  INTEGER,
    PRIMARY KEY (actorid, start_date, current_year)
);
