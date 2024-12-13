-- In this example, only yhe month of 2023-01 was checked with this dynamical BIT counting technique
WITH starter AS (
    SELECT uc.device_activity_datelist @> ARRAY [DATE(d.valid_date)]   AS is_active,
           EXTRACT(
               DAY FROM DATE('2023-01-31') - d.valid_date) AS days_since,
           uc.user_id,
           uc.browser_type
    FROM user_devices_cumulated uc
             CROSS JOIN
         (SELECT generate_series('2022-12-31', '2023-01-31', INTERVAL '1 day') AS valid_date) as d
),
bits AS (
    SELECT user_id,
        browser_type,
        SUM(CASE
                WHEN is_active THEN POW(2, 32 - days_since)
                ELSE 0 END)::bigint::bit(32) AS datelist_int
    FROM starter
    GROUP BY user_id, browser_type
)
SELECT
        user_id,
        datelist_int,
        browser_type,
        BIT_COUNT(datelist_int)>0 as monthly_active,
        BIT_COUNT(datelist_int) as num_days_active
FROM bits