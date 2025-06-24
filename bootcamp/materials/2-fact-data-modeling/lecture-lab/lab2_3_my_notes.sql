-- This query populates the users_cumulated table. We run it 31 times 
-- starting with yesterday's table when date 2022-12-31 and today's table when date
-- is 2023-01-01 until
-- yesterday 2023-01-30 and today 2023-01-31
INSERT into users_cumulated WITH yesterday AS (
        SELECT *
        FROM users_cumulated
        WHERE curr_date = DATE('2023-01-30')
    ),
    today AS (
        SELECT CAST(user_id AS TEXT) AS user_id,
            DATE(
                CAST(event_time AS TIMESTAMP)
            ) AS date_active
        FROM events
        WHERE DATE(
                CAST(event_time AS TIMESTAMP)
            ) = DATE('2023-01-31')
            AND user_id IS NOT NULL
        GROUP BY user_id,
            DATE(
                CAST(event_time AS TIMESTAMP)
            )
    )
SELECT COALESCE(t.user_id, y.user_id) AS user_id,
    CASE
        WHEN y.dates_active IS NULL THEN ARRAY [CAST(t.date_active AS TIMESTAMPTZ)]
        WHEN t.date_active IS NULL THEN y.dates_active
        ELSE ARRAY [CAST(t.date_active AS TIMESTAMPTZ)] || y.dates_active
    END AS dates_active,
    DATE(
        COALESCE(t.date_active, y.curr_date + INTERVAL '1 day')
    ) AS curr_date
FROM today t
    FULL OUTER JOIN yesterday y ON t.user_id = y.user_id
    /* In the lab, dates_active was an array of DATE. This got me wrong values.
     In the first insert into (when we full join users_cumulated when 
     curr_date is 2022-12-31 and today's table with date 2023-01-01), I got the value
     ["2022-12-31T23:00:00.000Z"] instead of ["2023-01-01T00:00:00.000Z"]. 
     There is a quick fix which is when adding dates to the array, writing
     -- ARRAY[ t.date_active +Interval '1 day']. 
     
     Trying both ARRAY [CAST(t.date_active AS TIMESTAMP)] and 
     ARRAY [CAST(t.date_active AS TIMESTAMPTZ)AT TIME ZONE 'UTC' ]
     gives: ["2022-12-31T23:00:00.000Z"] in both CTE result and insert into.
     However, each of ARRAY [CAST(t.date_active AS TIMESTAMPTZ)] and 
     ARRAY [CAST(t.date_active AS TIMESTAMP)AT TIME ZONE 'UTC']
     gives correct output in the CTE (gives ["2023-01-01T00:00:00.000Z"])
     But, after inserting the result of the CTE (using insert into as the first line
     of the query), the resulting users_cumulated table has the bad value again.
     Note the time_event column in events is already in UTC.
     */