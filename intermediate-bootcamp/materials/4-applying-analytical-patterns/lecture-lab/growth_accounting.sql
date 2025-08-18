WITH yesterday AS (
    SELECT * FROM users_growth_accounting
    WHERE date = DATE('2023-03-09')
),
     today AS (
         SELECT
            CAST(user_id AS TEXT) as user_id,
            DATE_TRUNC('day', event_time::timestamp) as today_date,
            COUNT(1)
         FROM events
         WHERE DATE_TRUNC('day', event_time::timestamp) = DATE('2023-03-10')
         AND user_id IS NOT NULL
         GROUP BY user_id, DATE_TRUNC('day', event_time::timestamp)
     )

         SELECT COALESCE(t.user_id, y.user_id)                    as user_id,
                COALESCE(y.first_active_date, t.today_date)       AS first_active_date,
                COALESCE(t.today_date, y.last_active_date)        AS last_active_date,
                CASE
                    WHEN y.user_id IS NULL THEN 'New'
                    WHEN y.last_active_date = t.today_date - Interval '1 day' THEN 'Retained'
                    WHEN y.last_active_date < t.today_date - Interval '1 day' THEN 'Resurrected'
                    WHEN t.today_date IS NULL AND y.last_active_date = y.date THEN 'Churned'
                    ELSE 'Stale'
                    END                                           as daily_active_state,
                CASE
                    WHEN y.user_id IS NULL THEN 'New'
                    WHEN y.last_active_date < t.today_date - Interval '7 day' THEN 'Resurrected'
                    WHEN
                            t.today_date IS NULL
                            AND y.last_active_date = y.date - interval '7 day' THEN 'Churned'
                    WHEN COALESCE(t.today_date, y.last_active_date) + INTERVAL '7 day' >= y.date THEN 'Retained'
                    ELSE 'Stale'
                    END                                           as weekly_active_state,
                COALESCE(y.dates_active,
                         ARRAY []::DATE[])
                    || CASE
                           WHEN
                               t.user_id IS NOT NULL
                               THEN ARRAY [t.today_date]
                           ELSE ARRAY []::DATE[]
                    END                                           AS date_list,
                COALESCE(t.today_date, y.date + Interval '1 day') as date
         FROM today t
                  FULL OUTER JOIN yesterday y
                                  ON t.user_id = y.user_id
