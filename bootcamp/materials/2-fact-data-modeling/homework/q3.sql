-- cumulative query to generate device_activity_datelist from events
INSERT INTO user_devices_cumulated
WITH yesterday AS (
    SELECT
        *
    FROM user_devices_cumulated WHERE date = DATE(:'2022-12-31')
), today AS (
    SELECT
        e.user_id,
        d.browser_type,
        DATE(CAST(e.event_time as TIMESTAMP)) AS date
    FROM events e
        LEFT JOIN devices d
            USING(device_id)
    WHERE DATE(CAST(event_time as TIMESTAMP)) = DATE(:'2023-01-01')
        AND user_id IS NOT NULL
        AND browser_type IS NOT NULL
    GROUP BY user_id, browser_type, DATE(CAST(e.event_time as TIMESTAMP))
)
SELECT
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(t.browser_type, y.browser_type) AS browser_type,
    CASE
        WHEN y.device_activity_datelist IS NULL
            THEN ARRAY[t.date]
        WHEN t.date IS NULL
            THEN y.device_activity_datelist
        ELSE ARRAY[t.date] || y.device_activity_datelist
    END AS device_activity_datelist,
    COALESCE(t.date, y.date + INTERVAL '1 day') AS date
FROM today t
    FULL OUTER JOIN yesterday y
        USING(user_id, browser_type)

SELECT date, COUNT(1) FROM user_devices_cumulated GROUP BY date ORDER BY date