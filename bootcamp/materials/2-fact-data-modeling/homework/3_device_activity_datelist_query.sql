INSERT INTO user_devices_cumulated
WITH yesterday AS (
    SELECT *
    FROM user_devices_cumulated
    WHERE record_date = DATE('2023-01-30')
),
events_ranked AS (
    SELECT *, 
        CAST(event_time AS DATE) AS event_date,
        ROW_NUMBER() OVER 
            (PARTITION BY user_id, device_id, CAST(event_time AS DATE) ORDER BY event_time) 
            AS row_num
    FROM events
    WHERE user_id IS NOT NULL 
),
today AS (
    SELECT
        e.user_id,
        e.event_date AS date_active,
        d.browser_type
    FROM events_ranked e
    JOIN devices d ON e.device_id = d.device_id
    WHERE e.event_date = DATE('2023-01-31') AND e.row_num = 1
    GROUP BY e.user_id, e.event_date, d.browser_type
)
SELECT
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(t.browser_type, y.browser_type) AS browser_type,
    CASE 
        WHEN y.device_activity_datelist IS NULL
            THEN ARRAY[t.date_active]
        WHEN t.date_active IS NULL
            THEN y.device_activity_datelist
        ELSE 
            ARRAY[t.date_active] || y.device_activity_datelist
    END
        AS device_activity_datelist,
    COALESCE(t.date_active, y.record_date + INTERVAL '1 DAY') AS record_date
FROM today t
FULL OUTER JOIN yesterday y
ON y.user_id = t.user_id AND y.browser_type = t.browser_type;