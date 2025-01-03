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
)
select *,
    ARRAY [series_date::TIMESTAMPTZ],
    dates_active @> ARRAY [series_date::TIMESTAMPTZ]
from users
    CROSS JOIN series
WHERE user_id = '137925124111668560';