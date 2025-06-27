CREATE TABLE actors_history_scd (
    actorid         TEXT NOT NULL,
    actor           TEXT NOT NULL,
    quality_class   quality_class_enum NOT NULL,
    is_active       BOOLEAN NOT NULL,
    start_date      DATE NOT NULL,
    end_date        DATE,
    PRIMARY KEY (actorid, start_date)
);
