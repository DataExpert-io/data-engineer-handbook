WITH users AS (
    SELECT *
    FROM user_devices_cumulated
    WHERE record_date = DATE('2023-01-30')
),
series AS (
    SELECT CAST(series_date AS DATE)
    FROM generate_series(DATE('2023-01-28'), DATE('2023-01-30'), INTERVAL '1 DAY')
    AS series_date
),
activity_check AS (
    SELECT
        user_id,
        browser_type,
        record_date,
        device_activity_datelist @> ARRAY[series_date] AS is_active,
        (record_date - series_date) AS days_since 
    FROM users CROSS JOIN series 
    -- WHERE user_id = '17358702759623100000'
),
activity_values AS (
    SELECT
        user_id,
        browser_type,
        record_date,
        CASE WHEN is_active
            THEN CAST(POW(2, (32 - days_since)) AS BIGINT) ELSE 0
        END AS int_value
    FROM activity_check
)
SELECT 
    user_id, 
    browser_type,
    record_date,
    CAST(CAST(SUM(int_value) AS BIGINT) AS BIT(32)) AS datelist_int
FROM activity_values 
GROUP BY user_id, browser_type, record_date;