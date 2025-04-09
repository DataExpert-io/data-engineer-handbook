-- here we study all cohorts of users.
-- This query is more details.
-- select date,
--     first_active_date,
--     date - first_active_date as days_since_first_active,
--     count(
--         CASE
--             when daily_active_state in ('Retained', 'Resurrected', 'New') then 1
--         end
--     ) as number_active,
--     cast(
--         count(
--             CASE
--                 when daily_active_state in ('Retained', 'Resurrected', 'New') then 1
--             end
--         ) as real
--     ) / count(1) as pct_active,
--     count(1)
-- from users_growth_accounting
-- GROUP BY date,
--     first_active_date
-- ORDER BY date ASC,
--     first_active_date ASC;
-- -- =====================================================================
-- select date - first_active_date as days_since_first_active,
--     count(
--         CASE
--             when daily_active_state in ('Retained', 'Resurrected', 'New') then 1
--         end
--     ) as number_active,
--     cast(
--         count(
--             CASE
--                 when daily_active_state in ('Retained', 'Resurrected', 'New') then 1
--             end
--         ) as real
--     ) / count(1) as pct_active,
--     count(1)
-- from users_growth_accounting
-- GROUP BY date - first_active_date;
-- ==================================================== 
-- let us see which day of the week is the most active
select EXTRACT(
        dow
        from first_active_date
    ) as day_of_week,
    date - first_active_date as days_since_first_active,
    count(
        CASE
            when daily_active_state in ('Retained', 'Resurrected', 'New') then 1
        end
    ) as number_active,
    cast(
        count(
            CASE
                when daily_active_state in ('Retained', 'Resurrected', 'New') then 1
            end
        ) as real
    ) / count(1) as pct_active,
    count(1)
from users_growth_accounting
GROUP BY EXTRACT(
        dow
        from first_active_date
    ),
    date - first_active_date
order by date - first_active_date asc,
    EXTRACT(
        dow
        from first_active_date
    ) asc;