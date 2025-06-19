-- We work with the events table. First, we create the users_cumulated table.
CREATE TABLE users_cumulated(
    user_id TEXT,
    --BIGINT,
    -- the list of dates in the past where the user was active
    dates_active TIMESTAMPTZ [],
    -- the current date for the user
    curr_date DATE,
    PRIMARY KEY(user_id, curr_date)
);