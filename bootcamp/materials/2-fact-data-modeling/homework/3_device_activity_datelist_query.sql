WITH yesterday AS (
    SELECT *
    FROM user_devices_cumulated
    WHERE record_date = DATE('2023-01-01')
),
today AS (
    SELECT e.*, d.browser_type
    FROM events e
    JOIN devices d ON e.device_id = d.device_id
    WHERE CAST(e.event_time AS DATE) = DATE('2023-01-01')
)
SELECT
    COALESCE(y.user_id, t.user_id) AS user_id,
    COALESCE(y.browser_type, t.browser_type) AS browser_type,
    NULL AS device_activity_datelist,
    NULL AS record_date
FROM yesterday y 
FULL OUTER JOIN today t
ON y.user_id = t.user_id;