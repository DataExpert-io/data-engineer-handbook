-- The incremental query to generate host_activity_datelist

INSERT INTO hosts_cumulated
WITH yesterday AS (
    SELECT
        *
    FROM hosts_cumulated WHERE date = DATE(:'2022-12-31')
), today AS (
    SELECT
        host,
        DATE(CAST(event_time as TIMESTAMP)) AS date
    FROM events
    WHERE DATE(CAST(event_time as TIMESTAMP)) = DATE(:'2023-01-01')
    GROUP BY host, DATE(CAST(event_time as TIMESTAMP))
)
SELECT
    COALESCE(t.host, y.host) AS user_id,
    CASE
        WHEN y.host_activity_datelist IS NULL
            THEN ARRAY[t.date]
        WHEN t.date IS NULL
            THEN y.host_activity_datelist
        ELSE ARRAY[t.date] || y.host_activity_datelist
    END AS host_activity_datelist,
    COALESCE(t.date, y.date + INTERVAL '1 day') AS date
FROM today t
    FULL OUTER JOIN yesterday y
        USING(host)

SELECT date, COUNT(1) FROM hosts_cumulated GROUP BY date ORDER BY date

SELECT * FROM hosts_cumulated