-- Each day, how many new, retained, churned, and resurrected users are there?
-- SELECT date,
--     daily_active_state,
--     COUNT(1)
-- from users_growth_accounting
-- GROUP BY date,
--     daily_active_state;
-- ======================================================================
-- Let us check number of rows for each date
-- select date,
--     count(1)
-- FROM users_growth_accounting
-- GROUP BY date;
-- =====================================================================
-- this slice of data has exactly 84 rows.
-- select *
-- from users_growth_accounting
-- where date = DATE('2023-01-02')
--     and first_active_date = DATE('2023-01-01');
-- =====================================================================
-- here there are 1260 rows = 84 x 15 (I filled in the first 15 days only)
-- SELECT *
-- from users_growth_accounting
-- where first_active_date = DATE('2023-01-01');
-- =====================================================================
-- For each date, the number of count is the same: 84.
-- This is a cohort of 84 users.
-- select date,
--     count(1)
-- from users_growth_accounting
-- where first_active_date = DATE('2023-01-01')
-- GROUP BY date;
-- =====================================================================
-- The J-Curve
select date,
    count(
        CASE
            when daily_active_state in ('Retained', 'Resurrected', 'New') then 1
        end
    ) as number_active,
    count(1) as num_users_in_cohort,
    cast(
        count(
            CASE
                when daily_active_state in ('Retained', 'Resurrected', 'New') then 1
            end
        ) as real
    ) / count(1) as pct_active
from users_growth_accounting
where first_active_date = DATE('2023-01-01')
GROUP BY date;
-- =====================================================================
select date - first_active_date as days_since_first_active,
    count(
        CASE
            when daily_active_state in ('Retained', 'Resurrected', 'New') then 1
        end
    ) as number_active,
    count(1) as num_users_in_cohort,
    cast(
        count(
            CASE
                when daily_active_state in ('Retained', 'Resurrected', 'New') then 1
            end
        ) as real
    ) / count(1) as pct_active
from users_growth_accounting
where first_active_date = DATE('2023-01-01')
GROUP BY date - first_active_date;
-- The previous two queries study only one cohort of users namely
-- the ones who started on 2023-01-01.