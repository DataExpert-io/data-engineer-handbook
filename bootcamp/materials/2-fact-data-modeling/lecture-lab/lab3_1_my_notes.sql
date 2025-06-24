-- CREATE TABLE array_metrics (
--     user_id NUMERIC,
--     month_start DATE,
--     metric_name TEXT,
--     metric_array REAL [],
--     PRIMARY KEY(user_id, month_start, metric_name)
-- )
INSERT into array_metrics with daily_aggregate AS(
        SELECT user_id,
            DATE(event_time) as current_date,
            COUNT(1) as num_site_hits
        from events
        where DATE(event_time) = DATE('2023-01-03')
            AND user_id is not null
        GROUP BY user_id,
            DATE(event_time)
    ),
    yesterday_array AS(
        SELECT *
        from array_metrics
        WHERE month_start = DATE('2023-01-01') -- This is a fixed date!
    )
SELECT COALESCE(da.user_id, ya.user_id) as user_id,
    COALESCE(
        ya.month_start,
        MAKE_DATE(
            EXTRACT(
                YEAR
                FROM da.current_date
            )::INT,
            EXTRACT(
                MONTH
                FROM da.current_date
            )::INT,
            1 -- Set the day to 1
        )
    ) as month_start,
    'site_hits' as metric_name,
    case
        when ya.metric_array is not null then ya.metric_array || array [COALESCE(da.num_site_hits,0)]
        when ya.metric_array is null then ARRAY(
            SELECT 0::INTEGER
            FROM generate_series(
                    1,
                    COALESCE(da.current_date::DATE - DATE('2023-01-01'), 0)
                )
        ) || array [COALESCE(da.num_site_hits,0)]
    end as metric_array
from daily_aggregate da
    FULL OUTER JOIN yesterday_array ya on da.user_id = ya.user_id ON CONFLICT (user_id, month_start, metric_name) DO
update
set metric_array = EXCLUDED.metric_array;
-- array_fill(0, current_date - month_start) doesn't work in postgres. I had to use:
-- when ya.metric_array is null then ARRAY(
--             SELECT 0::INTEGER
--             FROM generate_series(
--                     1,
--                     COALESCE(da.current_date::DATE - DATE('2023-01-01'), 0)
--                 )
--         ) || array [COALESCE(da.num_site_hits,0)]
-- To check that all users have N values in their array, run this:
-- SELECT cardinality(metric_array), count(1) from array_metrics GROUP BY 1;