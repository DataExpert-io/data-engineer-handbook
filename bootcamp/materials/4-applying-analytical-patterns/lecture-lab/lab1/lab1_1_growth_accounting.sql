-- Run this code cumutively as in week 1
INSERT INTO users_growth_accounting WITH yesterday AS (
        SELECT *
        FROM users_growth_accounting
        WHERE date = DATE('2023-01-14')
    ),
    today AS (
        SELECT CAST(user_id AS TEXT) as user_id,
            DATE_TRUNC('day', event_time::timestamptz) as today_date,
            COUNT(1)
        FROM events
        WHERE DATE_TRUNC('day', event_time::timestamptz) = DATE('2023-01-15')
            AND user_id IS NOT NULL
        GROUP BY user_id,
            DATE_TRUNC('day', event_time::timestamptz)
    )
SELECT COALESCE(t.user_id, y.user_id) as user_id,
    COALESCE(y.first_active_date, t.today_date) AS first_active_date,
    COALESCE(t.today_date, y.last_active_date) AS last_active_date,
    CASE
        WHEN y.user_id IS NULL THEN 'New'
        WHEN y.last_active_date = t.today_date - Interval '1 day' THEN 'Retained'
        WHEN y.last_active_date < t.today_date - Interval '1 day' THEN 'Resurrected'
        WHEN t.today_date IS NULL
        AND y.last_active_date = y.date THEN 'Churned'
        ELSE 'Stale'
    END as daily_active_state,
    CASE
        WHEN y.user_id IS NULL THEN 'New'
        WHEN y.last_active_date >= y.date - INTERVAL '7 day' THEN 'Retained'
        WHEN y.last_active_date < t.today_date - Interval '7 day' THEN 'Resurrected' -- resurrected means that their last active day was 7 days ago or more
        WHEN t.today_date IS NULL
        AND y.last_active_date = y.date - interval '7 day' THEN 'Churned'
        ELSE 'Stale'
    END as weekly_active_state,
    COALESCE(
        y.dates_active,
        ARRAY []::TIMESTAMPTZ []
    ) || CASE
        WHEN t.user_id IS NOT NULL THEN ARRAY [CAST(t.today_date as TIMESTAMPTZ)]
        ELSE ARRAY []::TIMESTAMPTZ []
    END AS dates_active,
    COALESCE(t.today_date, y.date + Interval '1 day') as date
FROM today t
    FULL OUTER JOIN yesterday y ON t.user_id = y.user_id;