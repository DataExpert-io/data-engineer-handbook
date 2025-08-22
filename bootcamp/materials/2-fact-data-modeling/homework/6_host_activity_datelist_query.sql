INSERT INTO hosts_cumulated
WITH yesterday AS (
    SELECT *
    FROM hosts_cumulated
    WHERE record_date = DATE('2023-01-02')
),
events_ranked AS (
    SELECT *,
        CAST(event_time AS DATE) AS event_date,
        ROW_NUMBER() OVER 
            (PARTITION BY user_id, host, CAST(event_time AS DATE) ORDER BY event_time) 
            AS row_num
    FROM events
    WHERE user_id IS NOT NULL 
),
today AS (
    SELECT
        user_id,
        host,
        event_date AS date_active
    FROM events_ranked
    WHERE event_date = DATE('2023-01-03') AND row_num = 1
)
SELECT
    COALESCE(y.user_id, t.user_id) AS user_id,
    COALESCE(y.host, t.host) AS host,
    CASE 
        WHEN y.host_activity_datelist IS NULL
            THEN ARRAY[t.date_active]
        WHEN t.date_active IS NULL
            THEN y.host_activity_datelist
        ELSE 
            ARRAY[t.date_active] || y.host_activity_datelist
    END
        AS host_activity_datelist,
    COALESCE(t.date_active, y.record_date + INTERVAL '1 DAY') AS record_date
FROM today t
FULL OUTER JOIN yesterday y
ON y.user_id = t.user_id AND y.host = t.host;