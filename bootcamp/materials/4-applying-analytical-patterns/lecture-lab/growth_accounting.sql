WITH yesterday AS (
    SELECT * FROM users_growth_accounting
    WHERE date = DATE('2023-03-09')
),

today AS (
    SELECT
        CAST(user_id AS TEXT) AS user_id,
        DATE_TRUNC('day', CAST(event_time AS TIMESTAMP)) AS today_date,
        COUNT(*)
    FROM events
    WHERE
        DATE_TRUNC('day', CAST(event_time AS TIMESTAMP)) = DATE('2023-03-10')
        AND user_id IS NOT NULL
    GROUP BY user_id, DATE_TRUNC('day', CAST(event_time AS TIMESTAMP))
)

SELECT
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(y.first_active_date, t.today_date) AS first_active_date,
    COALESCE(t.today_date, y.last_active_date) AS last_active_date,
    CASE
        WHEN y.user_id IS NULL THEN 'New'
        WHEN
            y.last_active_date = t.today_date - INTERVAL '1 day'
            THEN 'Retained'
        WHEN
            y.last_active_date < t.today_date - INTERVAL '1 day'
            THEN 'Resurrected'
        WHEN t.today_date IS NULL AND y.last_active_date = y.date THEN 'Churned'
        ELSE 'Stale'
    END AS daily_active_state,
    CASE
        WHEN y.user_id IS NULL THEN 'New'
        WHEN
            y.last_active_date < t.today_date - INTERVAL '7 day'
            THEN 'Resurrected'
        WHEN
            t.today_date IS NULL
            AND y.last_active_date = y.date - INTERVAL '7 day' THEN 'Churned'
        WHEN
            COALESCE(t.today_date, y.last_active_date) + INTERVAL '7 day'
            >= y.date
            THEN 'Retained'
        ELSE 'Stale'
    END AS weekly_active_state,
    COALESCE(
        y.dates_active,
        CAST(ARRAY[] AS DATE [])
    )
    || CASE
        WHEN
            t.user_id IS NOT NULL
            THEN ARRAY[t.today_date]
        ELSE CAST(ARRAY[] AS DATE [])
    END AS date_list,
    COALESCE(t.today_date, y.date + INTERVAL '1 day') AS date
FROM today AS t
FULL OUTER JOIN yesterday AS y
    ON t.user_id = y.user_id
