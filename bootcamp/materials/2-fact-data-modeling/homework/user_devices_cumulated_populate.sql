-- Here, we use the device table to retrieve the browser_type being used.
-- It will be useful during the aggregation phase
WITH today AS (
    SELECT
        e.user_id,
        d.browser_type,
        DATE_TRUNC('day', e.event_time::TIMESTAMP) AS today_date
    FROM events e
    INNER JOIN devices d
        ON d.device_id = e.device_id
    WHERE DATE_TRUNC('day', e.event_time::TIMESTAMP) = DATE('2023-01-20')
        AND e.user_id IS NOT NULL
    GROUP BY e.user_id,  d.browser_type, DATE_TRUNC('day', e.event_time::TIMESTAMP)
    )
INSERT INTO user_devices_cumulated (user_id, browser_type, device_activity_datelist)
SELECT
    COALESCE(t.user_id::TEXT, udc.user_id) as user_id,
    COALESCE(t.browser_type, udc.browser_type) AS browser_type,
    COALESCE(udc.device_activity_datelist, ARRAY[]::DATE[])
     || CASE WHEN t.user_id IS NOT NULL
            THEN ARRAY[t.today_date::DATE]
            ELSE ARRAY[]::DATE[]
        END
        AS device_activity_datelist
FROM user_devices_cumulated udc
    FULL OUTER JOIN
today t ON udc.user_id = t.user_id::TEXT AND udc.browser_type = t.browser_type
-- Handle conflicts by updating existing rows
ON CONFLICT (user_id,browser_type) DO UPDATE
SET
    user_id = EXCLUDED.user_id,
    browser_type = EXCLUDED.browser_type,
    device_activity_datelist = EXCLUDED.device_activity_datelist;

SELECT * FROM user_devices_cumulated