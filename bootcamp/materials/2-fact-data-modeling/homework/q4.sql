-- A datelist_int generation query. Convert the device_activity_datelist column into a datelist_int column


WITH user_devices AS (
    SELECT *
    FROM user_devices_cumulated WHERE date = :'2023-01-05'
), series AS (
    SELECT *
    FROM generate_series(DATE('2023-01-01'),  DATE('2023-01-31'), INTERVAL '1 day')
        AS series_date
), place_holder_ints AS (
    SELECT
        CASE
            WHEN device_activity_datelist @> ARRAY[DATE(series_date)]
                THEN CAST(POW(2, 32 - (date - DATE(series_date))) AS BIGINT)
            ELSE 0
        END AS placeholder_int_value,
        *
    FROM user_devices
        CROSS JOIN series
)
SELECT
    user_id,
    browser_type,
    CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS datelist_int

FROM place_holder_ints
GROUP BY user_id, browser_type
ORDER BY 2 DESC