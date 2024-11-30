---------- FDA Lab2 ----------------
/*
Working with event data 
trying to create a user activity table

*/

--SELECT * FROM events


------ Creating User cummulative table where we store user activity 
--DROP TABLE user_cumulative
-- CREATE TABLE user_cumulative (

--     user_id TEXT,

--     --List of Dates where user is active in past
--     dates_active DATE[],

--     -- current date of user
--     date DATE,
--     PRIMARY KEY(user_id,date)
-- )


--------- Back filling data from jan 1 jan 31

-- INSERT INTO user_cumulative
WITH yesterday AS (
    SELECT * FROM user_cumulative
    WHERE date = DATE('2023-03-30')
),

today AS (
          SELECT user_id,
                 DATE('day', event_time) AS today_date,
                 COUNT(1) AS num_events FROM events
            WHERE DATE('day', event_time) = DATE('2023-03-31')
            AND user_id IS NOT NULL
         GROUP BY user_id,  DATE('day', event_time)
    )

SELECT
       COALESCE(t.user_id, y.user_id),
       COALESCE(y.dates_active,
           ARRAY[]::DATE[])
            || CASE WHEN
                t.user_id IS NOT NULL
                THEN ARRAY[t.today_date]
                ELSE ARRAY[]::DATE[]
                END AS date_list,
       COALESCE(t.today_date, y.date + Interval '1 day') as date
FROm yesterday y
    FULL OUTER JOIN
    today t ON t.user_id = y.user_id;




--SELECT * FROM user_cumulative
--DELETE FROM user_cumulative WHERE TRUE

