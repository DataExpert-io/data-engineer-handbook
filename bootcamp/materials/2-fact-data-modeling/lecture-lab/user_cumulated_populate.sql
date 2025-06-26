WITH yesterday AS (
    SELECT * FROM users_cumulated
    WHERE date = DATE('2023-03-30')
),

today AS (
    SELECT
        user_id,
        DATE_TRUNC('day', event_time) AS today_date,
        COUNT(*) AS num_events
    FROM events
    WHERE
        DATE_TRUNC('day', event_time) = DATE('2023-03-31')
        AND user_id IS NOT NULL
    GROUP BY user_id, DATE_TRUNC('day', event_time)
)

INSERT INTO users_cumulated
SELECT
    COALESCE(t.user_id, y.user_id),
    COALESCE(
        y.dates_active,
        ARRAY[]::DATE []
    )
    || CASE
        WHEN
            t.user_id IS NOT NULL
            THEN ARRAY[t.today_date]
        ELSE ARRAY[]::DATE []
    END AS date_list,
    COALESCE(t.today_date, y.date + INTERVAL '1 day') AS date
FROM yesterday AS y
FULL OUTER JOIN
    today AS t ON y.user_id = t.user_id;
