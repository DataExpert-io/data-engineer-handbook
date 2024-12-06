SELECT 
    MAX(event_time), MIN(event_time)
FROM events


DROP TABLE users_cumulated;
CREATE TABLE users_cumulated (
    user_id TEXT,
        -- List of dates in the past where the user was active
    dates_active DATE[],
        -- Current date for the user
    date DATE,
    PRIMARY KEY (user_id, date)
);


INSERT INTO users_cumulated
WITH yesterday AS (
    SELECT * FROM users_cumulated WHERE date = DATE(:'2022-12-31')
), today AS (
    SELECT
        CAST(user_id AS TEXT) AS user_id,
        DATE(CAST(event_time AS TIMESTAMP)) AS date_active
    FROM events
    WHERE
        DATE(CAST(event_time AS TIMESTAMP)) = DATE(:'2023-01-31')
        AND user_id IS NOT NULL
    GROUP BY user_id, DATE(CAST(event_time AS TIMESTAMP))
)

SELECT
    COALESCE(t.user_id, y.user_id) as user_id,
    CASE
        WHEN y.dates_active IS NULL
            THEN ARRAY[t.date_active]
        WHEN t.date_active IS NULL
            THEN y.dates_active
        ELSE
            ARRAY[t.date_active] || y.dates_active
    END AS dates_active,
    COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
FROM today t
    FULL OUTER JOIN yesterday y
        USING(user_id);



WITH users AS (
    SELECT *
    FROM users_cumulated WHERE date = :'2023-01-05'
), series AS (
    SELECT *
    FROM generate_series(DATE('2023-01-01'),  DATE('2023-01-31'), INTERVAL '1 day')
        AS series_date
), place_holder_ints AS (
    SELECT
        CASE
            WHEN dates_active @> ARRAY[DATE(series_date)]
                THEN CAST(POW(2, 32 - (date - DATE(series_date))) AS BIGINT)
            ELSE 0
        END AS placeholder_int_value,
        *
    FROM users
        CROSS JOIN series
)
SELECT
    user_id,
    CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)),
    BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) AS dim_monthly_active

FROM place_holder_ints
GROUP BY user_id
ORDER BY 2 DESC


