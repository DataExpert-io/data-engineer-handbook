WITH events_augmented AS (
    SELECT
        url,
        user_id,
        COALESCE(d.os_type, 'unknown') AS os_type,
        COALESCE(d.device_type, 'unknown') AS device_type,
        COALESCE(d.browser_type, 'unknown') AS browser_type,
        CASE
            WHEN referrer LIKE '%linkedin%' THEN 'Linkedin'
            WHEN referrer LIKE '%t.co%' THEN 'Twitter'
            WHEN referrer LIKE '%google%' THEN 'Google'
            WHEN referrer LIKE '%lnkd%' THEN 'Linkedin'
            WHEN referrer LIKE '%eczachly%' THEN 'On Site'
            WHEN referrer LIKE '%zachwilson%' THEN 'On Site'
            ELSE referrer
        END AS referrer,
        DATE(event_time) AS event_date
    FROM events AS e
    INNER JOIN devices AS d ON e.device_id = d.device_id
),

aggregated AS (
    SELECT
        url,
        referrer,
        event_date,
        COUNT(*) AS count
    FROM events_augmented
    GROUP BY url, referrer, event_date
),

windowed AS (
    SELECT
        referrer,
        url,
        event_date,
        count,
        SUM(count) OVER (
            PARTITION BY referrer, url, DATE_TRUNC('month', event_date)
            ORDER BY event_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS monthly_cumulative_sum,
        SUM(count) OVER (
            PARTITION BY referrer, url
            ORDER BY event_date
        ) AS rolling_cumulative_sum,
        SUM(count) OVER (
            PARTITION BY referrer, url
            ORDER BY event_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS total_cumulative_sum,
        SUM(count) OVER (
            PARTITION BY referrer, url
            ORDER BY event_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS weekly_rolling_count,
        SUM(count) OVER (
            PARTITION BY referrer, url
            ORDER BY event_date
            ROWS BETWEEN 13 PRECEDING AND 6 PRECEDING
        ) AS previous_weekly_rolling_count
    FROM aggregated
    ORDER BY referrer, url, event_date
)

SELECT
    referrer,
    url,
    event_date,
    count,
    weekly_rolling_count,
    previous_weekly_rolling_count,
    CAST(count AS REAL) / monthly_cumulative_sum AS pct_of_month,
    CAST(count AS REAL) / total_cumulative_sum AS pct_of_total
FROM windowed
WHERE
    total_cumulative_sum > 500
    AND referrer IS NOT NULL
