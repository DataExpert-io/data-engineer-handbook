with users as (
    select *
    from users_cumulated
    where curr_date = '2023-01-31'
),
series as (
    SELECT *
    from generate_series(
            DATE('2023-01-01'),
            DATE('2023-01-31'),
            INTERVAL '1 day'
        ) as series_date
),
placeholder_ints AS (
    select user_id,
        dates_active,
        curr_date,
        DATE(series_date) as series_date,
        dates_active @> ARRAY [series_date::TIMESTAMPTZ] as did_connect,
        curr_date - DATE(series_date) as days_diff,
        case
            when dates_active @> ARRAY [series_date::TIMESTAMPTZ] then power(2, 32 - (curr_date - DATE(series_date)))
            else 0
        end as power_of_two,
        cast(
            case
                when dates_active @> ARRAY [series_date::TIMESTAMPTZ] then cast(
                    power(2, 32 - (curr_date - DATE(series_date))) as bigint
                )
                else 0
            end as bit(32)
        ) as placeholder_int_value
    from users
        CROSS JOIN series
    WHERE user_id = '439578290726747300'
) -- select *
-- from placeholder_ints;
select user_id,
    CAST(CAST(SUM(power_of_two) AS bigint) AS BIT(32))
from placeholder_ints
group by user_id;
-- Some other users id to use as examples:
-- 137925124111668560
--'406876712821807740'