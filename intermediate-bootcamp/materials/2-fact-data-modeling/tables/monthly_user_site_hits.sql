CREATE TABLE monthly_user_site_hits
(
    user_id          BIGINT,
    hit_array        BIGINT[],
    month_start      DATE,
    first_found_date DATE,
    date_partition DATE,
    PRIMARY KEY (user_id, date_partition, month_start)
);